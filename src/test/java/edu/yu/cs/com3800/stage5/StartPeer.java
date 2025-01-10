package edu.yu.cs.com3800.stage5;
import edu.yu.cs.com3800.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class StartPeer {
    public static void main(String[] args)throws IOException {
        int port = Integer.valueOf(args[0]);
        long id = Long.valueOf(args[1]);
        Map<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        peerIDtoAddress.put(10L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(11L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(12L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(13L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(14L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(15L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(16L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(17L, new InetSocketAddress("localhost", 8080));
        peerIDtoAddress.remove(id);
        PeerServerImpl peerServer = new PeerServerImpl(port, 0, id, peerIDtoAddress, 17L, 1);
        peerServer.start();
    }
}
