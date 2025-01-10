package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.*;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * defines the class that will be run on all the servers in the cluster
 */

public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {

    private final InetSocketAddress myAddress;
    private final int myPort;
    private final int numberOfObservers;
    private ServerState state;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Long> deadFollowers;
    private final Long id;
    private long peerEpoch;
    private final long gatewayID;
    private volatile Vote currentLeader;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private LeaderElection ourElection;
    private Gossiper ourGossiper;
    private Message cachedPreElectionWork;
    private boolean shutdown = false;

    private final Logger ourLogger;
    private final Logger Summary;
    private final Logger Verbose;
    private final String SummaryPath;
    private final String VerbosePath;

    private HttpServer SummaryServer;
    private HttpServer VerboseServer;
    private HttpServer VerbosePathServer;

    /**
     * @param udpPort the port we use to send and receive UDP messages
     * @param peerEpoch the current epoch at creation of the server
     * @param serverID ourID
     * @param peerIDtoAddress the map of serverIDs to the addresses
     * @param gatewayID the serverID of the gateway server
     * @param numberOfObservers number of observing servers that are not counted towards the quorum of servers
     * @throws IOException
     */
    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException {
        if(peerIDtoAddress == null){
            throw new IllegalArgumentException("peerIDtoAddress is null");
        }
        this.myPort = udpPort;
        this.peerEpoch = peerEpoch;
        this.id = serverID;
        this.gatewayID = gatewayID;
        this.numberOfObservers = numberOfObservers;
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress);
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.state = ServerState.LOOKING;
        this.ourLogger = initializeLogging(PeerServerImpl.class.getCanonicalName() + "-on-udpPort-" + this.myPort);
        HTTPLogger HttpLogger = new HTTPLogger();
        HTTPLogger.LoggerWithPath SummaryLog = HttpLogger.initializeLogging("Summary Log File ServerID-" + id);
        this.Summary = SummaryLog.logger();
        this.SummaryPath = SummaryLog.logFilePath();
        HTTPLogger.LoggerWithPath verboseLog = HttpLogger.initializeLogging("Verbose Log File ServerI-" + id);
        this.Verbose = verboseLog.logger();
        this.VerbosePath = verboseLog.logFilePath();
    }

    private void CreateLogHTTPServers(){
        try {
            VerbosePathServer = HttpServer.create(new InetSocketAddress(myPort+1), 0);
            SummaryServer = HttpServer.create(new InetSocketAddress(myPort+3), 0);
            VerboseServer = HttpServer.create(new InetSocketAddress(myPort+4), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        SummaryServer.createContext("/summary", exchange -> {
            File file = new File(SummaryPath);
            byte[] fileBytes = Files.readAllBytes(file.toPath());
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, fileBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(fileBytes);
            os.close();
        });
        VerboseServer.createContext("/verbose", exchange -> {
            File file = new File(VerbosePath);
            byte[] fileBytes = Files.readAllBytes(file.toPath());
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, fileBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(fileBytes);
            os.close();
        });
        VerbosePathServer.createContext("/verbose", exchange -> {
            byte[] fileBytes = VerbosePath.getBytes();
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, fileBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(fileBytes);
            os.close();
        });
        VerbosePathServer.start();
        SummaryServer.start();
        VerboseServer.start();
    }


    @Override
    public void run(){
        CreateLogHTTPServers();
        ourLogger.log(Level.FINE, "Called run() - Server #"+ id);
        //step 1: create and run thread that sends broadcast messages
        senderWorker = new UDPMessageSender(outgoingMessages, myPort);
        senderWorker.start();
        //step 2: create and run thread that listens for messages sent to this server
        try {
            receiverWorker = new UDPMessageReceiver(incomingMessages, myAddress, myPort, this);
            receiverWorker.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //step 3: main server loop
        try {
            ourGossiper = new Gossiper(incomingMessages, peerIDtoAddress, id, this, Summary, Verbose);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ourGossiper.start();
        boolean first = true;
        if(this.getPeerState() == ServerState.OBSERVER){
            ourLogger.log(Level.FINE, this.id + ": We are an OBSERVER");
        }
        try{
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        //start leader election, set leader to the election winner
                        if(!first){
                            peerEpoch++;
                        }
                        ourLogger.log(Level.FINE, "Looking for leader - Server #"+ id);
                        this.ourElection = new LeaderElection(this, incomingMessages, ourLogger);
                        Vote winner = ourElection.lookForLeader();
                        setCurrentLeader(winner);
                        first = false;
                        break;
                    case LEADING:
                        Summary.log(Level.FINE, this.id + ": switching from LOOKING to LEADING");
                        System.out.println(this.id + ": switching from LOOKING to LEADING");
                        executeLead();
                        break;
                    case FOLLOWING:
                        Summary.log(Level.FINE, this.id + ": switching from LOOKING to FOLLOWING");
                        System.out.println(this.id + ": switching from LOOKING to FOLLOWING");
                        executeFollow();
                        break;
                    case OBSERVER:
                        if(!first){
                            peerEpoch++;
                        }
                        this.ourElection = new LeaderElection(this, incomingMessages, ourLogger);
                        Vote winner2 = ourElection.lookForLeader();
                        setCurrentLeader(winner2);
                        executeObserve();
                        first = false;
                        break;
                    default:
                        ourLogger.log(Level.FINE, "Unknown peer state - Server #"+ id);
                        break;
                }
            }
        }
        catch (Exception e) {
            //code...
            ourLogger.log(Level.SEVERE, "PeerServerImpl.run()", e);
        }
    }

    private void executeObserve(){
        ourLogger.log(Level.FINE, "Observe, starting to execute - Server #"+ id);
        while(this.currentLeader != null) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * this is run if this server is elected leader of the cluster
     * runs until shutdown or stops being the leader
     */
    private void executeLead(){
        //make a roundRobin thread
        RoundRobinLeader ourRobin;
        deadFollowers = new LinkedBlockingQueue<>();
        try {
            ourRobin = new RoundRobinLeader(this, myAddress, peerIDtoAddress, gatewayID, numberOfObservers, deadFollowers);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ourRobin.start();
        while(!this.isInterrupted()){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                break;
            }
        }
        ourRobin.shutdown();
    }

    /**
     * this is run if this server is a worker server in the cluster
     * runs until shutdown or if stops being a follower
     */
    private void executeFollow(){
        //make a new follower thread
        JavaRunnerFollower myFollower;
        try {
            myFollower = new JavaRunnerFollower(myAddress, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        myFollower.start();
        while(this.currentLeader != null) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                break;
            }
        }
        myFollower.shutdown();
        //shutdown the follower
    }

    /**
     * adds a message to the cached list of messages to be sent after a failure
     * @param m
     */
    protected void addCachedAfterFailure(Message m){
        this.cachedPreElectionWork = m;
    }

    /**
     * gets and clears the list of cached messages to be sent to new leader after a failure
     * @return
     */
    protected Message getAndClearCachedAfterFailure(){
        Message temp = this.cachedPreElectionWork;
        this.cachedPreElectionWork = null;
        return temp;
    }

    @Override
    public void shutdown() {
        ourLogger.log(Level.FINE, "Shutting down - Server #"+ id);
        this.shutdown = true;
        ourGossiper.interrupt();
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        SummaryServer.stop(0);
        VerboseServer.stop(0);
        VerbosePathServer.stop(0);
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    /**
     * sends UDP message to a specific server
     * @param type of the message being sent
     * @param messageContents
     * @param target
     * @throws IllegalArgumentException
     */
    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        if(type == null || target == null || messageContents == null || messageContents.length == 0){
            throw new IllegalArgumentException("no null inputs allowed");
        }
        if(!peerIDtoAddress.containsValue(target)){
            ourLogger.log(Level.FINE, "server " + target + " not in cluster");
            throw new IllegalArgumentException("server not in cluster");
        }
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        this.outgoingMessages.offer(msg);
    }

    /**
     * broadcasts a message to all servers in the cluster
      * @param type of message being sent
     * @param messageContents
     */
    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        if(type == null || messageContents == null || messageContents.length == 0){
            throw new IllegalArgumentException("no null inputs allowed");
        }
        Collection<InetSocketAddress> peers = peerIDtoAddress.values();
        for(InetSocketAddress peer : peers)
        {
            Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, peer.getHostString(), peer.getPort());
            this.outgoingMessages.offer(msg);
        }
    }

    @Override
    public void reportFailedPeer(long peerID){
        Summary.log(Level.SEVERE, this.id + ": no heartbeat from server " + peerID + " - SERVER FAILED");
        System.out.println(this.id + ": no heartbeat from server " + peerID + " - SERVER FAILED");
        peerIDtoAddress.remove(peerID);
        if(currentLeader != null && currentLeader.getProposedLeaderID() == peerID){
            this.currentLeader = null;
            if(this.getPeerState() != ServerState.OBSERVER){
                this.state = ServerState.LOOKING;
                Summary.log(Level.FINE, this.id + ": switching from FOLLOWING to LOOKING");
                System.out.println(this.id + ": switching from FOLLOWING to LOOKING");
            }else{
                //System.out.println(this.id + ": we are in election");
            }
        }
        if(this.state == ServerState.LEADING){
            deadFollowers.offer(peerID);
        }
    }

    @Override
    public boolean isPeerDead(long peerID){
        if(!peerIDtoAddress.containsKey(peerID)){
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address){
        if(!peerIDtoAddress.containsValue(address)){
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return (peerIDtoAddress.size()-numberOfObservers+1)/2 + 1;
    }
}
