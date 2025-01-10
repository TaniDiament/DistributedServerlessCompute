package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.*;

public class Stage5UnitTest {
    static String program1 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello1\"; \n" +
            "    }\n" +
            "}";
    static String program2 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello2\"; \n" +
            "    }\n" +
            "}";
    static String program3 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello3\"; \n" +
            "    }\n" +
            "}";
    static String program4 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello4\"; \n" +
            "    }\n" +
            "}";
    static String program5 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello5\"; \n" +
            "    }\n" +
            "}";
    static String program6 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello6\"; \n" +
            "    }\n" +
            "}";
    static String program7 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello7\"; \n" +
            "    }\n" +
            "}";
    static String program8 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello8\"; \n" +
            "    }\n" +
            "}";
    static String program9 = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello9\"; \n" +
            "    }\n" +
            "}";
    static String program = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello\"; \n" +
            "    }\n" +
            "}";
    static String badProgram = "class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return \"Hello\"; \n" +
            "    }\n" +
            "}";
    static String nullProgram = "public class HelloWorld {\n" +
            "    public void HelloWorld()\n" +
            "    {\n" +
            "    }\n" +
            "    public String run()\n" +
            "    {return null; \n" +
            "    }\n" +
            "}";

    static int HttpPort = 8888;
    static HashMap<Long, InetSocketAddress> peerIDtoAddress;
    List<Callable<Client.Response>> callables;

    @BeforeEach
    public void setUp() {
        peerIDtoAddress = new HashMap<>(3);
        peerIDtoAddress.put(10L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(11L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(12L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(13L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(14L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(15L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(16L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(17L, new InetSocketAddress("localhost", 8080));
        callables = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            TestClient cl = new TestClient("localhost", HttpPort, program);
            callables.add(cl);
        }
    }
    @Test
    public void stage5Test() throws IOException, InterruptedException, ExecutionException {
        //setup all the servers
        PeerServerImpl leaderServer = null;
        PeerServerImpl server1 = null;
        ArrayList<PeerServerImpl> servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(entry.getKey() == 17){
                continue;
            }
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 17L, 1);
            if (entry.getKey() == 16) {
                leaderServer = server;
            }
            if (entry.getKey() == 10) {
                server1 = server;
            }
            servers.add(server);
        }
        HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
        ConcurrentHashMap<Long, InetSocketAddress> map2 = new ConcurrentHashMap<>(map);
        map2.remove(17L);
        GatewayServer ourGate = new GatewayServer(8888, 8080, 0, 17L, map2, 1);

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        //start all the servers
        ourGate.start();
        for(PeerServerImpl server : servers) {
            server.start();
        }

        try {
            Thread.sleep(8000);
        }
        catch (Exception e) {
        }

        for (PeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                assertEquals(16, leader.getProposedLeaderID());
                if(server.getServerId() == 16){
                    assertEquals(PeerServer.ServerState.LEADING, server.getPeerState());
                }else {
                    assertEquals(PeerServer.ServerState.FOLLOWING, server.getPeerState());
                }
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
        }
        Callable<Client.Response> first = new TestClient("localhost", HttpPort, program);
        Future<Client.Response> firstResponse = executorService.submit(first);
        Client.Response res = firstResponse.get();
        assertFalse(res.getCached());

        Callable<Client.Response> firstBadProgram = new TestClient("localhost", HttpPort, badProgram);
        Future<Client.Response> firstBadResponse = executorService.submit(firstBadProgram);
        Client.Response resBad = firstBadResponse.get();
        assertFalse(resBad.getCached());
        assertEquals(400, resBad.getCode());

        Callable<Client.Response> secondBadProgram = new TestClient("localhost", HttpPort, badProgram);
        Future<Client.Response> secondBadResponse = executorService.submit(secondBadProgram);
        Client.Response ressecondBad = secondBadResponse.get();
        assertTrue(ressecondBad.getCached());
        assertEquals(400, ressecondBad.getCode());

        List<Future<Client.Response>> futures = executorService.invokeAll(callables);
        List<Client.Response> responses = new ArrayList<>();

        for (Future<Client.Response> future : futures) {
            Client.Response response = future.get();
            responses.add(response);
        }
        for(Client.Response response : responses){
            assertEquals(200, response.getCode());
            assertEquals("Hello", response.getBody());
            assertTrue(response.getCached());
        }
        Thread.sleep(30000);

        GateStatusClient gateStatus = new GateStatusClient("localhost", 8889, "/leaderstatus");
        Client.Response gateRes = gateStatus.call();
        assertFalse(gateRes.getCached());
        System.out.println(gateRes.getCode()+gateRes.getBody());

        GateStatusClient gateSummary = new GateStatusClient("localhost", 8043, "/summary");
        GateStatusClient gateVerbose = new GateStatusClient("localhost", 8044, "/verbose");
        Client.Response gateSummaryRes = gateSummary.call();
        Client.Response gateVerboseRes = gateVerbose.call();
        FileWriter writer = new FileWriter(new File("summary.txt"));
        writer.write(gateSummaryRes.getBody());
        writer.close();
        writer = new FileWriter(new File("verbose.txt"));
        writer.write(gateVerboseRes.getBody());
        writer.close();
        executorService.shutdown();

        leaderServer.shutdown();
        servers.remove(leaderServer);
        Thread.sleep(33000);
        TestClient client2 = new TestClient("localhost", HttpPort, program5);
        Client.Response response2 = client2.call();
        if(response2 == null){
            System.out.println("Response is null");
            servers.add(ourGate.getPeerServer());
            for (PeerServerImpl server : servers) {
                server.shutdown();
            }
            ourGate.shutdown();
            return;
        }
        System.out.println("Request:");
        System.out.println(response2.getSrc());
        System.out.println("Response:");
        System.out.println(response2.getBody());
        Thread.sleep(70000);

        List<String> programs = new ArrayList<>();
        programs.add(program1);
        programs.add(program2);
        programs.add(program3);
        programs.add(program4);
        programs.add(program5);
        programs.add(program6);
        programs.add(program7);
        programs.add(program8);
        programs.add(program9);
        List<TestClient> callables = new ArrayList<>();
        for(String program : programs){
            TestClient cl = new TestClient("localhost", HttpPort, program);
            callables.add(cl);
        }

        server1.shutdown();
        servers.remove(server1);
        Thread.sleep(30000);

        callables.parallelStream().forEach(cl -> {
            Client.Response responseNew = cl.call();
            System.out.println("Request:");
            System.out.println(responseNew.getSrc());
            System.out.println("Response:");
            System.out.println(responseNew.getBody());
        });


        servers.add(ourGate.getPeerServer());
        for (PeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
        }

        for (PeerServerImpl server : servers) {
            server.shutdown();
        }
        ourGate.shutdown();
    }
}