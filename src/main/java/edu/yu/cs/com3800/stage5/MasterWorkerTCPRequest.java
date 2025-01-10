package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * This class is a Runnable used by the roundRobin leader to establish a TCP connection with a worker server
 * to complete the request that was sent from the HTTP gateway (used in threadPool).
 */

public class MasterWorkerTCPRequest implements Runnable, LoggingServer {
    private final int socketPort;
    private Message request;
    private String address;
    private InetSocketAddress gatewayAddress;
    private WorkOrder workOrder;
    private Logger ourLogger;
    private PeerServerImpl ourServer;

    public MasterWorkerTCPRequest(String address, int socketPort, InetSocketAddress gatewayAddress, Message request, WorkOrder workOrder, PeerServerImpl ourServer) {
        this.socketPort = socketPort;
        this.request = request;
        this.gatewayAddress = gatewayAddress;
        this.address = address;
        this.workOrder = workOrder;
        this.ourServer = ourServer;
        try {
            this.ourLogger = initializeLogging(MasterWorkerTCPRequest.class.getCanonicalName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 1) send work to the worker over TCP socket
     * 2) get the response from the worker
     * 3) make a message to send to the gateWay
     * 4) send completedWork to the gateway via new TCP socket
     */
    @Override
    public void run() {
        int sendPort = request.getReceiverPort()+2;
        String sendHost = request.getSenderHost();
        byte[] data = request.getNetworkPayload();
        Message finishedWork;
        try {
            //send work to worker
            ourLogger.log(Level.FINE, "Sending to " + sendHost + ":" + sendPort + " from " + gatewayAddress.getPort());
            Socket ourSocket = new Socket(sendHost, sendPort);
            OutputStream outputStream = ourSocket.getOutputStream();
            outputStream.write(data);
            outputStream.flush();
            //get result
            InputStream inputStream = ourSocket.getInputStream();
            byte[] result = Util.readAllBytesFromNetwork(inputStream);
            inputStream.close();
            ourSocket.close();
            if(ourServer.isPeerDead(new InetSocketAddress(request.getReceiverHost(), request.getReceiverPort()))) {
                return;
            }
            workOrder.complete();
            //make new message from result
            Message response = new Message(result);
            ourLogger.log(Level.FINE, "Received " + response.toString());
            finishedWork = new Message(Message.MessageType.COMPLETED_WORK, response.getMessageContents(), address, socketPort,
                    gatewayAddress.getHostName(), gatewayAddress.getPort(), response.getRequestID(), response.getErrorOccurred());
            //connect to gateway and send response
            ourLogger.log( Level.FINE,"sending to port" + gatewayAddress.getPort());
            Socket gateSock = new Socket(gatewayAddress.getHostName(), gatewayAddress.getPort()+2);
            OutputStream gateOutputStream = gateSock.getOutputStream();
            result = finishedWork.getNetworkPayload();
            gateOutputStream.write(result);
            outputStream.flush();
        } catch (IOException e) {
            return;
        }
    }
}
