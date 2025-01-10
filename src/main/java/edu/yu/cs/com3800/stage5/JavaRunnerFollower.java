package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * defines the class for the workers of the cluster that receive work via TCP
 */

public class JavaRunnerFollower extends Thread implements LoggingServer{
    private final InetSocketAddress myAddress;
    private Logger ourLogger;
    private JavaRunner ourRunner;
    private PeerServerImpl myServer;
    private boolean error;
    private Message queuedWork;
    private ServerSocket ourSocket;
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss:SSS");

    public JavaRunnerFollower(InetSocketAddress myAddress, PeerServerImpl myServer) throws IOException {
        this.myAddress = myAddress;
        this.myServer = myServer;
        ourLogger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-udpPort-" + this.myAddress.getPort());
        queuedWork = myServer.getAndClearCachedAfterFailure();
    }

    @Override
    public void run() {
        if(queuedWork != null){
            sendCachedToLeader(queuedWork);
        }
        ourLogger.log(Level.FINE, "Starting Java Runner on " + myAddress.getPort());
        try {
            ourRunner = new JavaRunner();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            ourSocket = new ServerSocket(myAddress.getPort()+2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Socket incomingSocket = null;
        OutputStream outputStream = null;
        InputStream incomingData = null;
        while(!this.isInterrupted() && myServer.getCurrentLeader() != null) {
            error = false;
            Message nextTask;
            try{
                incomingSocket = ourSocket.accept();
                ourLogger.log(Level.FINE, "got mail");
                incomingData = incomingSocket.getInputStream();
                byte[] incomingDataBuffer = Util.readAllBytesFromNetwork(incomingData);
                nextTask = new Message(incomingDataBuffer);
            }catch(Exception e){
                break;
            }
            if(nextTask.getMessageType() == Message.MessageType.WORK){
                byte[] result = getResult(nextTask);
                Message resultMessage = new Message(Message.MessageType.COMPLETED_WORK, result, myAddress.getHostName(), myAddress.getPort(),
                        nextTask.getSenderHost(), nextTask.getSenderPort(), nextTask.getRequestID(), error);
                if(myServer.getCurrentLeader() == null){
                    myServer.addCachedAfterFailure(resultMessage);
                    break;
                }
                byte[] outGoingBytes = resultMessage.getNetworkPayload();
                try {
                    outputStream = incomingSocket.getOutputStream();
                    outputStream.write(outGoingBytes);
                    outputStream.flush();
                    ourLogger.log(Level.FINE, "Sent result");
                } catch (Exception e) {
                    break;
                }
            }
        }
        try {
            if(incomingData != null){
                incomingData.close();
            }
            if(outputStream != null){
                outputStream.close();
            }
            if(incomingSocket != null){
                incomingSocket.close();
            }
            ourSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown(){
        this.interrupt();
        try {
            ourSocket.close();
        } catch (IOException e) {
            ourLogger.log(Level.SEVERE, "Exception in socket closing");
            return;
        }
    }

    private void sendCachedToLeader(Message message){
        InetSocketAddress leaderAddress = myServer.getPeerByID(myServer.getCurrentLeader().getProposedLeaderID());
        try {
            Socket ourSocket = new Socket(leaderAddress.getHostName(), leaderAddress.getPort()+2);
            OutputStream outputStream = ourSocket.getOutputStream();
            outputStream.write(message.getNetworkPayload());
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * uses JavaRunner class to run the java code
     * @param nextTask
     * @return
     */
    private byte[] getResult(Message nextTask) {
        ourLogger.log(Level.FINE, "Worker on port: " + this.myAddress.getPort() + " received a new task");
        byte[] data = nextTask.getMessageContents();
        ourLogger.log(Level.FINE, new String(data));
        InputStream in = new ByteArrayInputStream(data);
        String response;
        try {
            response = ourRunner.compileAndRun(in);
            if(response == null){
                response = "null";
            }
            String time = dtf.format(now);
            ourLogger.log(Level.FINE, "code compiled and ran successfully at: "+time);
        } catch (Exception e) {
            error = true;
            response = e.getMessage();
            response = response + '\n';
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(byteArrayOutputStream);
            e.printStackTrace(ps);
            String output = byteArrayOutputStream.toString();
            response += output;
            ourLogger.log(Level.FINE,e.getMessage() + " code did not compile and run properly" + '\n' + output);
        }
        return response.getBytes();
    }
}
