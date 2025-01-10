package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;


/**
 * PeerServer used by the GateWay to watch th election and know who the current leader in the cluster is
 */
public class GatewayPeerServerImpl extends PeerServerImpl {
    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int observers) throws IOException {
        super(myPort, peerEpoch, id, peerIDtoAddress, id, observers);
    }

    @Override
    public ServerState getPeerState() {
        return ServerState.OBSERVER;
    }
}
