package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;

/**
 * Class to keep track if a work order has been completed, used in case of server failure
 */
public class WorkOrder {
    private final byte[] content;
    private final InetSocketAddress workerAddress;
    private final long requestID;
    private volatile boolean completed;

    public WorkOrder(InetSocketAddress workerAddress, byte[] content, long requestID){
        this.workerAddress = workerAddress;
        this.content = content;
        this.requestID = requestID;
    }

    public void complete(){
        completed = true;
    }

    public boolean isCompleted(){
        return completed;
    }

    public byte[] getContent() {
        return content;
    }

    public InetSocketAddress getWorkerAddress() {
        return workerAddress;
    }


    public long getRequestID() {
        return requestID;
    }
}
