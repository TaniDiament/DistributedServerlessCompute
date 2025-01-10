package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread used by all servers in the cluster to send and receive heartbeats
 */
public class Gossiper extends Thread implements LoggingServer {

    private static final int GOSSIP = 50;
    private static final int FAIL = GOSSIP * 600;
    private static final int CLEANUP = FAIL * 2;

    private long ourCount;
    private long[][] vectorClock;
    private final long myID;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private List<InetSocketAddress> Dead;
    private LinkedBlockingQueue<Message> incomingMessages;
    private final PeerServerImpl ourServer;
    private Logger Summary;
    private Logger Verbose;

    /**
     * Constructor
     * @param IM incoming UDP messages of heartbeats
     * @param PITA peerIDtoAddress map
     * @param myID my server ID
     * @param ourServer the mother ship ;)
     * @throws IOException
     */
    public Gossiper(LinkedBlockingQueue<Message> IM, ConcurrentHashMap<Long, InetSocketAddress> PITA, long myID, PeerServerImpl ourServer, Logger Summary, Logger Verbose) throws IOException {
        ourCount = 1;
        this.peerIDtoAddress = PITA;
        this.myID = myID;
        vectorClock = new long[1][3];
        this.incomingMessages = IM;
        this.ourServer = ourServer;
        this.Summary = Summary;
        this.Verbose = Verbose;
    }

    /**
     * main logic for Gossiping
     */
    @Override
    public void run() {
        Verbose.log( Level.FINE, "Gossiper started");
        this.waitDuringElection();
        Dead = new ArrayList<>();
        vectorClock[0][0] = ourCount;
        vectorClock[0][1] = System.currentTimeMillis();
        vectorClock[0][2] = myID;
        sendHeartBeat();
        long timeSinceWeSpoke = System.currentTimeMillis();
        while(!this.isInterrupted()) {
            //every three seconds send a heartbeat
            this.waitDuringElection();
            if(System.currentTimeMillis() >= timeSinceWeSpoke+GOSSIP) {
                ourCount++;
                vectorClock[0][1] = System.currentTimeMillis();
                vectorClock[0][0] = ourCount;
                sendHeartBeat();
                timeSinceWeSpoke = System.currentTimeMillis();
                checkForDeadPeers();
            }
            this.waitDuringElection();
            Message msg;
            try {
                msg = incomingMessages.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                break;
            }
            if(msg != null) {
                InetSocketAddress sender = new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort());
                if(msg.getMessageType() != Message.MessageType.GOSSIP || ourServer.isPeerDead(sender)) {
                    continue;
                }else{
                    if(Dead.contains(sender)) {
                        continue;
                    }
                    long[][] theirClock = getVCFromBytes(msg.getMessageContents());
                    long timeNow = System.currentTimeMillis();
                    Verbose.log( Level.FINE, "Received heartbeat from server " + getIDFromAddress(sender)
                            + ". Contents: " + getStringFromClock(theirClock) + " Time: " + timeNow);
                    for (long[] longs : theirClock) {
                        long ID = longs[2];
                        if (ourServer.isPeerDead(ID)) {
                            continue;
                        }
                        check(ID, longs);
                        int ourIndex = getIndexOfID(ID);
                        if (longs[0] > vectorClock[ourIndex][0]) {
                            Summary.log(Level.FINE, myID + ": updated " + ID + "'s heartbeat sequence to " + longs[0] +
                                    " based on message from " + getIDFromAddress(sender) + " at node time " + timeNow);
                            vectorClock[ourIndex][0] = longs[0];
                            vectorClock[ourIndex][1] = timeNow;
                        }
                    }
                }
            }
        }
    }

    /**
     * Iterate through the vector clock to check if a peer as not sent a heartbeat
     */
    private void checkForDeadPeers(){
        long timeSinceWeSpoke = System.currentTimeMillis();
        for(int i = 1; i < vectorClock.length; i++) {
            long lastTime = vectorClock[i][1];
            if(lastTime <= timeSinceWeSpoke-FAIL) {
                Verbose.log( Level.FINE, "marked dead " + vectorClock[i][2]);
                markDead(peerIDtoAddress.get(vectorClock[i][2]));
            }
            if(lastTime <= timeSinceWeSpoke-CLEANUP) {
                Verbose.log( Level.INFO, "cleaned up " + vectorClock[i][2]);
                Dead.remove(peerIDtoAddress.get(vectorClock[i][2]));
                removeRowFromVectorClock(vectorClock[i][2]);
                i--;
            }
        }
    }

    private int getIndexOfID(long ID){
        for(int i = 0; i < vectorClock.length; ++i){
            if(vectorClock[i][2] == ID){
                return i;
            }
        }
        return -1;
    }

    /**
     * Checks if we have this server in the vector clock and if we do not add it to te clock
     * @param ID
     * @param row
     */
    private void check(long ID, long[] row){
        boolean found = false;
        for (long[] longs : vectorClock) {
            if (longs[2] == ID) {
                found = true;
                break;
            }
        }
        if(!found){
            addRowToVectorClock(row);
        }
    }

    private void addRowToVectorClock(long[] newRow){
        long[][] newVC = new long[vectorClock.length+1][3];
        for(int i = 0; i < vectorClock.length; ++i){
            newVC[i] = vectorClock[i];
        }
        newVC[vectorClock.length] = newRow;
        vectorClock = newVC;
    }

    private void removeRowFromVectorClock(long ID) {
        int index = getIndexOfID(ID);
        if (index == -1) {
            return; // ID not found
        }
        long[][] newVC = new long[vectorClock.length - 1][3];
        int j = 0;
        for (int i = 0; i < vectorClock.length; i++) {
            if (i != index) {
                newVC[j] = vectorClock[i];
                j++;
            }
        }
        vectorClock = newVC;
    }

    private String getStringFromClock(long[][] clock){
        StringBuilder sb = new StringBuilder();
        sb.append(System.lineSeparator());
        sb.append("Count-Time-ID").append(System.lineSeparator());
        for (long[] row : clock) {
            sb.append(row[0]).append("-");
            sb.append(row[1]).append("-");
            sb.append(row[2]);
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }

    private long getIDFromAddress(InetSocketAddress Ad){
        long ID = 0;
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getValue().equals(Ad)) {
                ID =  entry.getKey();
                break;
            }
        }
        return ID;
    }

    /**
     * Pause gossip during election
     */
    private void waitDuringElection(){
        //make sure we are not in election
        if(ourServer.getCurrentLeader() == null){
            //System.out.println("waiting during election "+myID);
            while(ourServer.getCurrentLeader() == null){
                try {
                    sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
//            try{
//                System.out.println("done waiting during election "+myID + " leader is:" + ourServer.getCurrentLeader().getProposedLeaderID());
//            }catch (NullPointerException e){
//                System.out.println("done waiting during election "+myID + " leader is null");
//            }
            for(int i = 0; i < vectorClock.length; i++){
                vectorClock[i][1] = System.currentTimeMillis();
            }
        }
    }

    /**
     * Send a heartbeat to a random peer
     */
    private void sendHeartBeat(){
        List<InetSocketAddress> livePeers = new ArrayList<>(peerIDtoAddress.values());
        livePeers.remove(peerIDtoAddress.get(myID));
        if(livePeers.isEmpty()){
            throw new RuntimeException("No live peers to send heartbeat to");
        }
        int random = new Random().nextInt(livePeers.size());
        InetSocketAddress target = livePeers.get(random);
        ourServer.sendMessage(Message.MessageType.GOSSIP, getVCinBytes(), target);
    }

    /**
     * returns a byte[] of our vector clock to send over a message
     * @return
     */
    private byte[] getVCinBytes(){
        byte[] content = new byte[vectorClock.length * 3 * Long.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(content);
        for (long[] row : vectorClock) {
            for (long value : row) {
                buffer.putLong(value); // Write each long as 8 bytes
            }
        }
        return content;
    }

    /**
     * creates a peer's vector clock from the message's byte[]
     * @param content
     * @return
     */
    private long[][] getVCFromBytes(byte[] content){
        int length = content.length / (3 * Long.BYTES);//get the number of rows
        long[][] longArray = new long[length][3];
        ByteBuffer buffer = ByteBuffer.wrap(content);
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < 3; j++) {
                longArray[i][j] = buffer.getLong(); // Read 8 bytes as a long
            }
        }
        return longArray;
    }

    private void markDead(InetSocketAddress server){
        long cancel = 0;
        boolean found = false;
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getValue().equals(server)) {
                cancel =  entry.getKey();
                found = true;
                break;
            }
        }
        if(found && !Dead.contains(server)){
            ourServer.reportFailedPeer(cancel);
            Dead.add(server);
        }
    }
}
