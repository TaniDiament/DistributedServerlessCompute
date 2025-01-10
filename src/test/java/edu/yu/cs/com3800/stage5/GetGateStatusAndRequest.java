package edu.yu.cs.com3800.stage5;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GetGateStatusAndRequest {
    static int HttpPort = 8888;
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
    public static void main(String[] args) {
        GateStatusClient gatewayClient = new GateStatusClient("localhost", 8889, "/leaderstatus");
        Client.Response response = gatewayClient.call();
        int code = response.getCode();
        while(code == 503){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            response = gatewayClient.call();
            code = response.getCode();
        }
        if(code == 200){
            System.out.println(response.getBody());
        }
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
        List<Callable<Client.Response>> callables = new ArrayList<>();
        for(String program : programs){
            TestClient cl = new TestClient("localhost", HttpPort, program);
            callables.add(cl);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<Future<Client.Response>> futures;
        try {
            futures = executorService.invokeAll(callables);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<Client.Response> responses = new ArrayList<>();
        for (Future<Client.Response> future : futures) {
            try {
                Client.Response responseNew = future.get();
                responses.add(responseNew);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        executorService.shutdown();
        for(Client.Response responseNew : responses){
            System.out.println("Request:");
            System.out.println(responseNew.getSrc());
            System.out.println("Response:");
            System.out.println(responseNew.getBody());
        }
    }
}
