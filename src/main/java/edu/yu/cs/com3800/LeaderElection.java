package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

/**We are implementing a simplified version of the election algorithm. For the complete version which covers all possible scenarios, see https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 6000;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently, 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;

    private PeerServer ourServer;
    private LinkedBlockingQueue incomingMessages;
    private int quorumSize;
    private long ourProposedLeader;
    private long ourEpoch;
    private final boolean observeOnly;
    private Map<Long, ElectionNotification> votes;
    Logger logger;

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        this.ourServer = server;
        this.incomingMessages = incomingMessages;
        this.logger = logger;
        this.quorumSize = ourServer.getQuorumSize();
        this.ourProposedLeader = server.getServerId();
        this.ourEpoch = server.getPeerEpoch();
        if(server.getPeerState() == PeerServer.ServerState.OBSERVER) {
            observeOnly = true;
            ourProposedLeader = 0;
        }else {
            observeOnly = false;
        }
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        votes = new HashMap<>();
        int notificationInterval = 1875;
        try {
            //send initial notifications to get things started
            ElectionNotification notification = new ElectionNotification(ourServer.getServerId(), ourServer.getPeerState(), ourServer.getServerId(), ourServer.getPeerEpoch());
            byte[] byteArray = buildMsgContent(notification);
            if(!observeOnly){
                votes.put(ourServer.getServerId(), notification);
            }
            //sendNotifications();
            logger.log(Level.FINE, "Sending first ElectionNotifications");
            ourServer.sendBroadcast(Message.MessageType.ELECTION, byteArray);

            //Loop in which we exchange notifications with other servers until we find a leader
            while(true){
                //Remove next notification from queue
                Message next = (Message) incomingMessages.poll(finalizeWait/2, TimeUnit.MILLISECONDS);
                //If no notifications received...
                //...resend notifications to prompt a reply from others
                //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                if(next == null){
                    //need to wait some time maybe
                    Thread.sleep(notificationInterval);
                    if(notificationInterval < maxNotificationInterval){
                        notificationInterval = notificationInterval * 2;
                    }
                    ourServer.sendBroadcast(Message.MessageType.ELECTION, byteArray);
                    //System.out.println("Server " + ourServer.getServerId() + " is resending ElectionNotifications for leader " + ourProposedLeader);
                    logger.log(Level.FINE, "Server " + ourServer.getServerId() + " is resending ElectionNotifications for leader " + ourProposedLeader);
                }else {//if not null
                    //If we did get a message...
                    if(next.getMessageType() == Message.MessageType.GOSSIP){//ignore Gossip during election
                        continue;
                    }
                    ElectionNotification nextNote = getNotificationFromMessage(next);//get the note from the msg
                    //...if it's for an earlier epoch, or from an observer, ignore it.
                    if(nextNote.getPeerEpoch() < ourEpoch || nextNote.getState().equals(PeerServer.ServerState.OBSERVER)){
                        continue; //ignore the message
                    }else {
                        logger.log(Level.FINE, "Received ElectionNotification from Server "+ nextNote.getSenderID() + " for Server " + nextNote.getProposedLeaderID());
                        //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
                        long sender = nextNote.getSenderID();
                        long proposedLeader = nextNote.getProposedLeaderID();
                        votes.put(sender, nextNote);
                        if(supersedesCurrentVote(proposedLeader, nextNote.getPeerEpoch())){//we need to change ourVote
                            //update ourMaps
                            notification = new ElectionNotification(proposedLeader, ourServer.getPeerState(), ourServer.getServerId(), nextNote.getPeerEpoch());
                            if(!observeOnly){
                                votes.put(ourServer.getServerId(), notification);
                            }
                            this.ourProposedLeader = proposedLeader;
                            this.ourEpoch = nextNote.getPeerEpoch();
                            byteArray = buildMsgContent(notification);
                            ourServer.sendBroadcast(Message.MessageType.ELECTION, byteArray);//send our new vote
                            logger.log(Level.FINE, "Sending new ElectionNotifications");
                        }
                        Vote proposal = new Vote(this.ourProposedLeader, ourEpoch);
                        //If I have enough votes to declare my currently proposed leader as the leader...
                        if(haveEnoughVotes(votes, proposal)){
                            //..do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
                            boolean foundGreater = false;
                            Thread.sleep(finalizeWait);
                            //System.out.println("Server " + ourServer.getServerId() + " is checking for higher ranked leaders");
                            logger.log(Level.FINE, "Server " + ourServer.getServerId() + " is checking for higher ranked leaders");
                            while(incomingMessages.peek() != null){
                                Message peeked = (Message) incomingMessages.peek();
                                ElectionNotification peekedNote = getNotificationFromMessage(peeked);
                                if(peekedNote.getProposedLeaderID() > this.ourProposedLeader){
                                    foundGreater = true;
                                    break;
                                }else{
                                    incomingMessages.poll(finalizeWait/2, TimeUnit.MILLISECONDS);
                                }
                            }
                            if(foundGreater){
                                //System.out.println("Server " + ourServer.getServerId() + " found a higher ranked leader from " + this.ourProposedLeader);
                                logger.log(Level.FINE, "Server " + ourServer.getServerId() + " found a higher ranked leader from " + this.ourProposedLeader);
                                continue;
                            }else{
                                //System.out.println("Server " + ourServer.getServerId() + " did not find a higher ranked leader");
                                logger.log(Level.FINE, "Server " + ourServer.getServerId() + " did not find a higher ranked leader");
                                logger.log(Level.FINE, "Found leader: Server ID " + this.ourProposedLeader);
                                //declare leader
                                acceptElectionWinner(notification);
                                //System.out.println("Server " + ourServer.getServerId() + " is declaring the leader: " + ourProposedLeader);
                                logger.log(Level.FINE, "Server " + ourServer.getServerId() + " is declaring the leader: " + ourProposedLeader);
                                return proposal;
                                //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            this.logger.log(Level.SEVERE,"Exception occurred during election; election canceled",e);
        }
        return null;
    }


    /**
     * Takes a notification and returns the byte contents of the message
     * @param notification notification we want to make message from
     * @return
     */
    public static byte[] buildMsgContent(ElectionNotification notification) {
        char state = switch (notification.getState()) {
            case LOOKING -> 'O';
            case LEADING -> 'E';
            case FOLLOWING -> 'F';
            case OBSERVER -> 'B';
        };
        long leaderID = notification.getProposedLeaderID();
        long senderID = notification.getSenderID();
        long peerEpoch = notification.getPeerEpoch();
        ByteBuffer buffer = ByteBuffer.allocate(Character.BYTES + Long.BYTES * 3);

        // Put the values into the buffer
        buffer.putChar(state);
        buffer.putLong(leaderID);
        buffer.putLong(senderID);
        buffer.putLong(peerEpoch);

        // Get the byte array
        byte[] byteArray = buffer.array();
        return byteArray;
    }

    /**
     * Takes a message and gets the ElectionNotification from its bytes
     * @param msg
     * @return
     */
    public static ElectionNotification getNotificationFromMessage(Message msg) {
        byte[] messageBytes = msg.getMessageContents();
        ByteBuffer inputBuffer = ByteBuffer.wrap(messageBytes);
        // Read the values in the same order as they were written
        char deserializedState = inputBuffer.getChar();
        long deserializedLeaderID = inputBuffer.getLong();
        long deserializedSenderID = inputBuffer.getLong();
        long deserializedPeerEpoch = inputBuffer.getLong();
        PeerServer.ServerState sgState = PeerServer.ServerState.getServerState(deserializedState);
        ElectionNotification notification = new ElectionNotification(deserializedLeaderID, sgState, deserializedSenderID, deserializedPeerEpoch);
        return notification;
    }

    /**
     * Declare this notification as the winning vote
     * @param n
     * @return
     */
    private Vote acceptElectionWinner(ElectionNotification n) {
        //System.out.println("Server " + ourServer.getServerId() + " is accepting the election winner: " + n.getProposedLeaderID());
        logger.log(Level.FINE, "Server " + ourServer.getServerId() + " is accepting the election winner: " + n.getProposedLeaderID());
        //set my state to either LEADING or FOLLOWING
        long winnerId = n.getProposedLeaderID();
        if(winnerId == ourServer.getServerId()){
            ourServer.setPeerState(PeerServer.ServerState.LEADING);
        }else if(ourServer.getPeerState() != PeerServer.ServerState.OBSERVER){
            ourServer.setPeerState(PeerServer.ServerState.FOLLOWING);
        }
        //clear out the incoming queue before returning
        incomingMessages.clear();
        Vote winner = new Vote(winnerId, n.getPeerEpoch());
        return winner;
    }

    /**
     * We return true if one of the following two cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.ourServer.getPeerEpoch()) || ((newEpoch == this.ourServer.getPeerEpoch()) && (newId > this.ourProposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        long proposedLeader = proposal.getProposedLeaderID();
        int counter = 0;
        Collection<ElectionNotification> notifications = votes.values();
        for(ElectionNotification notification : notifications){
            if(notification.getProposedLeaderID() == proposedLeader){
                counter++;
            }
        }
        //System.out.println("Counter for Server " + ourServer.getServerId() + ": " + counter + " Quorum size: " + quorumSize + " Proposed leader: " + proposedLeader);
        logger.log(Level.FINE, "Counter for Server " + ourServer.getServerId() + ": " + counter + " Quorum size: " + quorumSize + " Proposed leader: " + proposedLeader);
        return counter >= quorumSize;
    }
}