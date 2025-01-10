package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class StartGateWay {
    public static void main(String[] args) throws IOException {
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();
        peerIDtoAddress.put(10L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(11L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(12L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(13L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(14L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(15L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(16L, new InetSocketAddress("localhost", 8010));
        GatewayServer ourGate = new GatewayServer(8888, 8080, 0, 17L, peerIDtoAddress, 1);
        ourGate.start();
    }
}
