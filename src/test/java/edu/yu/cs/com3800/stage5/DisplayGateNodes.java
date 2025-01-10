package edu.yu.cs.com3800.stage5;

public class DisplayGateNodes {
    private static final int GOSSIP = 200;
    public static void main(String[] args) {
        try {
            Thread.sleep(GOSSIP*175);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        GateStatusClient gatewayClient = new GateStatusClient("localhost", 8889, "/leaderstatus");
        Client.Response response = gatewayClient.call();
        int code = response.getCode();
        if (code == 200) {
            System.out.println(response.getBody());
        }
    }
}
