package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the GateWay server that accepts all the HttpRequests coming in to the system
 * The GateWay then sends a message over TCP to the Master in the cluster to compute response
 * The GateWay caches previous requests for efficiency
 */

public class GatewayServer extends Thread implements LoggingServer {
    private Logger ourLogger;
    private ConcurrentHashMap<Integer, String> cachedResponses;
    private ConcurrentHashMap<Integer, Integer> cachedCode;
    private volatile InetSocketAddress leaderAddress;
    private final int httpPort;
    private final int peerPort;
    private long peerEpoch;
    private final Long serverID;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ConcurrentHashMap<Long, Message> responseMap;
    private ConcurrentHashMap<HttpExchange, Long> exchangeMap;
    private int numberOfObservers;
    private Long leaderID;
    private HttpServer ourServer;
    private HttpServer statusServer;
    private AtomicLong requestCounter;
    private GatewayPeerServerImpl ourMan;

    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID,
                         ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException{
        this.httpPort = httpPort;
        this.peerPort = peerPort;
        this.peerEpoch = peerEpoch;
        this.serverID = serverID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.numberOfObservers = numberOfObservers;
        this.ourLogger = initializeLogging(GatewayServer.class.getCanonicalName() + "-on-udpPort-" + this.peerPort);
        requestCounter = new AtomicLong(1);
        responseMap = new ConcurrentHashMap<>();
        cachedResponses = new ConcurrentHashMap<>();
        cachedCode = new ConcurrentHashMap<>();
        exchangeMap = new ConcurrentHashMap<>();
    }

    /**
     * Starts the Http Server with a thread pool to process requests
     * runs until interrupted
     */
    @Override
    public void run() {
        try {
            ourMan = new GatewayPeerServerImpl(peerPort, peerEpoch, serverID, peerIDtoAddress, numberOfObservers);
            ourMan.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        setUpStatusEndPoint();
        while(ourMan.getCurrentLeader() == null){
            try {
                sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        leaderID = ourMan.getCurrentLeader().getProposedLeaderID();
        leaderAddress = peerIDtoAddress.get(leaderID);
        try {
            ourServer = HttpServer.create(new InetSocketAddress(httpPort), 10);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        GatewayTCPReceiver ourReceive;
        try {
            ourReceive = new GatewayTCPReceiver(peerPort+2, responseMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ourReceive.start();

        ourServer.createContext("/compileandrun", new Handler(peerPort, ourMan));
        int numberOfCores = Runtime.getRuntime().availableProcessors();
        ourServer.setExecutor(Executors.newFixedThreadPool(numberOfCores * 2));
        ourServer.start();
        while(!this.isInterrupted()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                break;
            }
        }
        ourReceive.interrupt();
    }

    public void setUpStatusEndPoint(){
        try {
            statusServer = HttpServer.create(new InetSocketAddress(httpPort+1), 5);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        statusServer.createContext("/leaderstatus", exchange -> {
            StringBuilder response;
            if(ourMan.getCurrentLeader() == null){
                response = new StringBuilder("No Leader yet");
                exchange.sendResponseHeaders(503, response.length());
            }else{
                response = new StringBuilder("Current Leader: " + ourMan.getCurrentLeader().getProposedLeaderID() + "\n");
                Set<Long> followers = peerIDtoAddress.keySet();
                followers.remove(ourMan.getCurrentLeader().getProposedLeaderID());
                response.append("Followers: ");
                for(Long follower : followers){
                    response.append(follower).append(" ");
                }
                exchange.sendResponseHeaders(200, response.length());
            }
            OutputStream os = exchange.getResponseBody();
            os.write(response.toString().getBytes());
            os.close();
        });
        statusServer.start();
    }


    public void shutdown(){
        this.interrupt();
        this.ourServer.stop(0);
        this.statusServer.stop(0);
        this.getPeerServer().shutdown();
    }

    public GatewayPeerServerImpl getPeerServer(){
        return this.ourMan;
    }


    /**
     * defines the HttpHandler class that will be used in a thread pool to process requests
     */
    private class Handler implements HttpHandler {

        private final int peerPort;
        private final PeerServer ourServer;

        public Handler(int peerPort, PeerServer ourServer){
            this.peerPort = peerPort;
            this.ourServer = ourServer;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            List<String> f = new ArrayList<>();
            f.add("false");
            List<String> t = new ArrayList<>();
            t.add("true");
            if(!exchange.getRequestMethod().equals("POST")) {//Checks that this is the correct HTTP request type
                exchange.getResponseHeaders().put("Cached-Response", f);
                exchange.sendResponseHeaders(405, 0);
                ourLogger.log(Level.FINE,exchange.getRequestMethod() + " unaccepted HTTP method, this server only accepts POST requests");
                OutputStream os = exchange.getResponseBody();
                os.close();
                return;
            }
            ourLogger.fine(exchange.getRequestMethod() + " accepted");
            Headers ourHeaders = exchange.getRequestHeaders();
            List<String> content = ourHeaders.get("Content-Type");
            String response;
            if(!content.getFirst().equals("text/x-java-source")) {
                response = "Invalid Content-Type";
                exchange.getResponseHeaders().put("Cached-Response", f);
                exchange.sendResponseHeaders(400, response.length());
                OutputStream os = exchange.getResponseBody();
                ourLogger.log(Level.FINE,content.getFirst()+" Invalid content type");
                os.write(response.getBytes());
                os.close();
                return;
            }
            byte[] requestBody = exchange.getRequestBody().readAllBytes();
            Integer requestHash = new String(requestBody).hashCode();
            if(cachedResponses.containsKey(requestHash)) {//check if response is cached
                response = cachedResponses.get(requestHash);
                exchange.getResponseHeaders().put("Cached-Response", t);
                exchange.sendResponseHeaders(cachedCode.get(requestHash), response.length());
                ourLogger.log(Level.FINE,"sent back response");
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }else{
                long requestNumber;
                if(exchangeMap.get(exchange) != null && exchangeMap.get(exchange) != 0){
                   requestNumber = exchangeMap.get(exchange);
                }else {
                    requestNumber = requestCounter.getAndIncrement();
                }
                exchangeMap.put(exchange, requestNumber);
                ourLogger.log(Level.FINE,"sent TCP");
                while(ourServer.getCurrentLeader() == null){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                Message responeMessage = sendOverTCPtoLeader(requestBody, requestNumber, peerIDtoAddress.get(ourServer.getCurrentLeader().getProposedLeaderID()));
                if(responeMessage == null){
                    return;
                }else {
                    exchangeMap.put(exchange, 0L);
                }
                response = new String(responeMessage.getMessageContents());
                ourLogger.fine("got TCP");
                if(responeMessage.getErrorOccurred()) {
                    exchange.getResponseHeaders().put("Cached-Response", f);
                    exchange.sendResponseHeaders(400, response.length());
                    cachedCode.put(requestHash, 400);
                }else{
                    exchange.getResponseHeaders().put("Cached-Response", f);
                    exchange.sendResponseHeaders(200, response.length());
                    cachedCode.put(requestHash, 200);
                }
                cachedResponses.put(requestHash, response);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        }

        private Message sendOverTCPtoLeader(byte[] streamBytes, long requestNumber, InetSocketAddress leaderAddress) throws IOException {
            ourLogger.log(Level.FINE, "called this method sending to " + leaderAddress);
            Socket ourSocket = new Socket(leaderAddress.getHostName(), (leaderAddress.getPort()+2));
            Message ourMessage = new Message(Message.MessageType.WORK, streamBytes, "localhost", peerPort, leaderAddress.getHostName(), leaderAddress.getPort(), requestNumber);
            byte[] data = ourMessage.getNetworkPayload();
            OutputStream outputStream = ourSocket.getOutputStream();
            outputStream.write(data);
            outputStream.flush();
            ourLogger.log(Level.FINE,"sent TCP to master ");
            //get result
            while(!responseMap.containsKey(requestNumber)){
                if(ourMan.isPeerDead(leaderAddress)){
                    return null;
                }
                try {
                    ourLogger.log(Level.FINE,"waiting for response");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if(ourMan.isPeerDead(leaderAddress)){
                return null;
            }else {
                return responseMap.get(requestNumber);
            }
        }

    }

}
