package edu.yu.cs.com3800.stage5;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.*;

import java.util.*;
import java.util.concurrent.*;

public class Stage5DemoTest {
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
    static List<String> programs;
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
        programs = new ArrayList<>();
        programs.add(program1);
        programs.add(program2);
        programs.add(program3);
        programs.add(program4);
        programs.add(program5);
        programs.add(program6);
        programs.add(program7);
        programs.add(program8);
        programs.add(program9);
        for(String program : programs){
            TestClient cl = new TestClient("localhost", HttpPort, program);
            callables.add(cl);
        }
    }
    @Test
    public void stage5Test() throws IOException, InterruptedException, ExecutionException {
        //step 2:
        //setup all the servers

        Map<Long, Process> serverProcesses = new HashMap<>();

        // Start the Gateway server
        ProcessBuilder gatewayBuilder = new ProcessBuilder("java", "-cp", "target/test-classes:target/classes", "edu.yu.cs.com3800.stage5.StartGateWay");
        Process gatewayProcess = gatewayBuilder.start();
        serverProcesses.put(17L, gatewayProcess);

        // Start each PeerServer in its own JVM
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getKey() == 17) {
                continue;
            }
            ProcessBuilder serverBuilder = new ProcessBuilder("java", "-cp", "target/test-classes:target/classes", "edu.yu.cs.com3800.stage5.StartPeer", String.valueOf(entry.getValue().getPort()), String.valueOf(entry.getKey()));
            Process serverProcess = serverBuilder.start();
            serverProcesses.put(entry.getKey(), serverProcess);
        }


        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()/2);
        int code = 0;
        //step 3:
        while (code != 200) {
            GateStatusClient gateStatus = new GateStatusClient("localhost", 8889, "/leaderstatus");
            Client.Response gateRes = gateStatus.call();
            code = gateRes.getCode();
            System.out.println(gateRes.getCode()+gateRes.getBody());
            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
        }

        int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
        List<String> verbosePaths = new ArrayList<>();
        for (int port : ports) {
            GateStatusClient gateStatus = new GateStatusClient("localhost", port+1, "/verbose");
            Client.Response gateRes = gateStatus.call();
            verbosePaths.add(gateRes.getBody());
        }


        //step 4:
        List<Client.Response> responses3 = new ArrayList<>();
        for(String program : programs){
            TestClient cl = new TestClient("localhost", HttpPort, program);
            responses3.add(cl.call());
        }

        for(Client.Response responseNew : responses3){
            System.out.println("Request:");
            System.out.println(responseNew.getSrc());
            System.out.println("Response:");
            System.out.println(responseNew.getBody());
        }

        //step 5:
        Process s1 = serverProcesses.get(10L);
        s1.destroy();
        serverProcesses.remove(10L);

        Thread.sleep(40000);

        GateStatusClient gateStatus = new GateStatusClient("localhost", 8889, "/leaderstatus");
        Client.Response gateRes = gateStatus.call();
        System.out.println(gateRes.getCode()+gateRes.getBody());

        //step 6:
        Process l1 = serverProcesses.get(16L);
        l1.destroy();
        serverProcesses.remove(16L);

        Thread.sleep(1000);

        List<Future<Client.Response>> futures = executorService.invokeAll(callables);

        //step 7:
        code = 0;
        while (code != 200) {
            GateStatusClient gateStatus2 = new GateStatusClient("localhost", 8889, "/leaderstatus");
            Client.Response gateRes2 = gateStatus2.call();
            code = gateRes2.getCode();
            System.out.println(gateRes2.getCode()+gateRes2.getBody());
            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
            }
        }
        List<Client.Response> responses = new ArrayList<>();
        for (Future<Client.Response> future : futures) {
            Client.Response response = future.get();
            responses.add(response);
        }
        executorService.shutdown();
        for(Client.Response responseNew : responses){
            System.out.println("Request:");
            System.out.println(responseNew.getSrc());
            System.out.println("Response:");
            System.out.println(responseNew.getBody());
        }

        //step 8:
        TestClient cl = new TestClient("localhost", HttpPort, nullProgram);
        Client.Response responseFinal = cl.call();
        System.out.println("Request:");
        System.out.println(responseFinal.getSrc());
        System.out.println("Response:");
        System.out.println(responseFinal.getBody());


        //step 9:
        System.out.println("Gossip paths:");
        for(String path : verbosePaths){
            System.out.println(path);
        }

        //step 10:
        for (Map.Entry<Long, Process> entry : serverProcesses.entrySet()) {
            entry.getValue().destroy();
        }
    }
}