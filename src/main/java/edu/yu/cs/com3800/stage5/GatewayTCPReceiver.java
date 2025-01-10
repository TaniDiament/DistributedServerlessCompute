package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * This class is use by the RoundRobin Leader to receive messages from the GateWayServer.
 * 1) Read a message from the InputStream
 * 2) Put it on the queue for the round Robin to get the next request
 */

public class GatewayTCPReceiver extends Thread implements LoggingServer{
    private ServerSocket ourSocket;
    private int listenPort;
    private ConcurrentHashMap<Long, Message> responseMap;

    private Logger ourLogger;

    public GatewayTCPReceiver(int listenPort, ConcurrentHashMap<Long, Message> responseMap) throws IOException {
        this.listenPort = listenPort;
        this.responseMap = responseMap;
        this.ourLogger = initializeLogging(GatewayTCPReceiver.class.getCanonicalName() + "-on-udpPort-" + this.listenPort);
    }

    /**
     * Runs in a loop until interrupted
     * Continuously accepts messages over TCP from the GateWayServer
     */
    @Override
    public void run() {
        try{
            ourLogger.fine("Listening on port " + this.listenPort);
            ourSocket = new ServerSocket(listenPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        while(!this.isInterrupted()){
            try {
                Socket incomingSocket = ourSocket.accept();
                InputStream incomingData = incomingSocket.getInputStream();
                byte[] incomingDataBuffer = Util.readAllBytesFromNetwork(incomingData);
                incomingData.close();
                incomingSocket.close();
                ourLogger.fine("received data");
                Message message = new Message(incomingDataBuffer);
                responseMap.put(message.getRequestID(), message);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
