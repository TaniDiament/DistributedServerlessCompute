package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * This class is use by the RoundRobin Leader to receive messages from the GateWayServer.
 * 1) Read a message from the InputStream
 * 2) Put it on the queue for the round Robin to get the next request
 */

public class TCPMessageReceiver extends Thread implements LoggingServer{
    private ServerSocket ourSocket;
    private int listenPort;
    private LinkedBlockingQueue<Message> messagesReceivedQueue;

    private Logger ourLogger;

    /**
     * Constructor for this
     * @param listenPort the port we are accepting connections on
     * @param messagesReceivedQueue the queue that will send the requests back to the roundRobinLeader
     * @throws IOException
     */
    public TCPMessageReceiver(int listenPort, LinkedBlockingQueue<Message> messagesReceivedQueue) throws IOException {
        this.listenPort = listenPort;
        this.messagesReceivedQueue = messagesReceivedQueue;
        this.ourLogger = initializeLogging(TCPMessageReceiver.class.getCanonicalName() + "-on-udpPort-" + this.listenPort);
    }

    /**
     * Runs in a loop until interrupted
     * Continuously accepts messages over TCP from the GateWayServer
     */
    @Override
    public void run() {
        try{
            ourSocket = new ServerSocket(listenPort);
            ourLogger.fine("Listening on port " + listenPort);
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
                messagesReceivedQueue.add(message);
            } catch (IOException e) {
                break;
            }
        }
        try {
            ourSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
