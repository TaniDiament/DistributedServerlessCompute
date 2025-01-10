package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer{
    private final static int finalizeWait = 3200;
    private final static int maxNotificationInterval = 30000;
    private ConcurrentHashMap<Long,InetSocketAddress> workerAddresses;
    private Map<Long, List<WorkOrder>> workOrderTracker;
    private final Long gatewayID;
    private int numberOfObservers;
    private Logger ourLogger;
    private final int myPort;
    private final String myHostName;
    private List<InetSocketAddress> workers;
    private Map<Long, Message> cachedPreElectionWork;
    private final PeerServerImpl ourServer;
    private ThreadPoolExecutor threadPool;
    private final InetSocketAddress gateWayAddress;
    private LinkedBlockingQueue<Long> deadFollowers;

    /**
     * Constructs a RoundRobinLeader instance to manage work distribution among a set of workers
     * using a round-robin load balancing strategy.
     *
     * @param ourServer The PeerServerImpl instance representing the current server.
     * @param myAddress The InetSocketAddress of the current server.
     * @param workerAddresses A map containing worker IDs as keys and their corresponding InetSocketAddress as values.
     * @param gatewayID The ID of the gateway worker.
     * @param numberOfObservers The number of observers monitoring the system.
     * @throws IOException If an I/O error occurs during initialization.
     */
    public RoundRobinLeader(PeerServerImpl ourServer, InetSocketAddress myAddress, ConcurrentHashMap<Long,InetSocketAddress> workerAddresses, Long gatewayID, int numberOfObservers, LinkedBlockingQueue<Long> deadFollowers) throws IOException {
        this.workerAddresses = workerAddresses;
        this.myPort = myAddress.getPort();
        this.gatewayID = gatewayID;
        this.numberOfObservers = numberOfObservers;
        this.myHostName = myAddress.getHostName();
        this.workers = new ArrayList<>(workerAddresses.values());
        this.workOrderTracker = new HashMap<>();
        this.deadFollowers = deadFollowers;
        gateWayAddress = workerAddresses.get(gatewayID);
        workers.remove(gateWayAddress);
        HashSet<Long> keys  = new HashSet<>(workerAddresses.keySet());
        for(Long workerAddress : keys) {
            workOrderTracker.put(workerAddress, new ArrayList<>());
        }
        this.ourServer = ourServer;
        this.ourLogger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-udpPort-" + this.myPort);
    }

    @Override
    public void run() {
        ourLogger.log(Level.FINE, "RoundRobinLeader started on port: " + this.myPort);
        int notificationInterval = 1875;
        cachedPreElectionWork = new HashMap<>();
        Message ours = ourServer.getAndClearCachedAfterFailure();
        if(ours != null){
            cachedPreElectionWork.put(ours.getRequestID(), ours);
        }
        LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
        TCPMessageReceiver ourReceiver;
        try {
            ourLogger.log(Level.FINE, "RoundRobinLeader starting TCP on port: " + this.myPort+2);
            ourReceiver = new TCPMessageReceiver(myPort+2, incomingMessages);
            ourReceiver.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int threadPoolSize = Runtime.getRuntime().availableProcessors();
        threadPool = new ThreadPoolExecutor(threadPoolSize, threadPoolSize*2, finalizeWait, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        while(!this.isInterrupted()) {
            Message nextRequest;
            try {
                nextRequest = incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                break;
            }
            if(nextRequest == null) {
                try {
                    Thread.sleep(notificationInterval);
                } catch (InterruptedException e) {
                    break;
                }
                if(notificationInterval < maxNotificationInterval){
                    notificationInterval = notificationInterval * 2;
                }
            }else if(nextRequest.getMessageType() == Message.MessageType.WORK){
                notificationInterval = 1875;
                //if we have the information from last election send it to the gateway
                if(cachedPreElectionWork.containsKey(nextRequest.getRequestID())){
                    sendToGateway(cachedPreElectionWork.get(nextRequest.getRequestID()));
                }else{
                    ourLogger.log(Level.FINE, "Job " + nextRequest.getRequestID() + " being assigned.");
                    InetSocketAddress nextWorker = this.getNextWorker(nextRequest.getRequestID());
                    ourLogger.log(Level.FINE, "Worker " + nextWorker.getHostName() + ":" + nextWorker.getPort() + " assigned task");
                    Message nextMessageWorkRequest = new Message(Message.MessageType.WORK, nextRequest.getMessageContents(), myHostName,
                            myPort, nextWorker.getHostName(), nextWorker.getPort(), nextRequest.getRequestID());
                    WorkOrder nextOrder = new WorkOrder(nextWorker, nextRequest.getMessageContents(), nextRequest.getRequestID());
                    workOrderTracker.get(getKeyFromValue(nextWorker)).add(nextOrder);
                    Runnable nextRunnable = new MasterWorkerTCPRequest(this.myHostName, this.myPort, gateWayAddress, nextMessageWorkRequest, nextOrder, ourServer);
                    threadPool.execute(nextRunnable);
                }
            }else if(nextRequest.getMessageType() == Message.MessageType.COMPLETED_WORK){
                cachedPreElectionWork.put(nextRequest.getRequestID(), nextRequest);
            }
        }
        threadPool.shutdown();
        ourReceiver.interrupt();
    }

    private Long getKeyFromValue(InetSocketAddress value){
        for(Map.Entry<Long, InetSocketAddress> entry : workerAddresses.entrySet()){
            if(entry.getValue().equals(value)){
                return entry.getKey();
            }
        }
        return null;
    }

    private void sendToGateway(Message message){
        try {
            Socket ourSocket = new Socket(gateWayAddress.getHostName(), gateWayAddress.getPort());
            OutputStream outputStream = ourSocket.getOutputStream();
            outputStream.write(message.getNetworkPayload());
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InetSocketAddress getNextWorker(long requestID){
        InetSocketAddress nextWorker = workers.get((int) (requestID % workers.size()));
        if(ourServer.isPeerDead(nextWorker)){
            dealWithFailed();
            workers.remove(nextWorker);
            if(workers.isEmpty()){
                ourLogger.log(Level.SEVERE, "all workers dead");
                throw new RuntimeException("all machines down mayday mayday we are going down");
            }
            nextWorker = getNextWorker(requestID);
        }
        return nextWorker;
    }

    /**
     * Reassigns incomplete work orders from the specified worker to other available workers
     * and removes the worker from the work order tracker.
     *
     * @param worker The InetSocketAddress of the worker whose work orders need to be reassigned.
     */
    private void makeUp(Long worker){
        ourLogger.log(Level.FINE, "Worker " + worker + " failed reassigning work");
        List<WorkOrder> workOrders = workOrderTracker.get(worker);
        if (workOrders == null) {
            return;
        }
        for(WorkOrder workOrder : workOrders){
            if(!workOrder.isCompleted()){
                InetSocketAddress nextWorker = this.getNextWorker(workOrder.getRequestID());
                ourLogger.log(Level.FINE, "Worker " + nextWorker.getHostName() + ":" + nextWorker.getPort() + " assigned task");
                Message nextMessageWorkRequest = new Message(Message.MessageType.WORK, workOrder.getContent(), myHostName,
                        myPort, nextWorker.getHostName(), nextWorker.getPort(), workOrder.getRequestID());
                WorkOrder nextOrder = new WorkOrder(nextWorker, workOrder.getContent(), workOrder.getRequestID());
                workOrderTracker.get(getKeyFromValue(nextWorker)).add(nextOrder);
                Runnable nextRunnable = new MasterWorkerTCPRequest(this.myHostName, this.myPort, gateWayAddress, nextMessageWorkRequest, nextOrder, ourServer);
                threadPool.execute(nextRunnable);
            }
        }
        workOrderTracker.remove(worker);
    }

    /**
     * Reassigns incomplete work orders from failed workers to other available workers.
     */
    private void dealWithFailed(){
        if(deadFollowers.peek() == null){
            return;
        }
        while(deadFollowers.peek() != null){
            long next = 0;
            try {
                next = deadFollowers.poll(5, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                return;
            }
            makeUp(next);
        }
    }

    public void shutdown() {
        ourLogger.log(Level.FINE, "RoundRobinLeader shutting down on port: " + myPort);
        this.interrupt();
    }
}
