package edu.yu.cs.com3800.stage5;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Callable;

public class GateStatusClient implements Client, Callable<Client.Response> {
    private Client.Response ourResponse;
    private final HttpClient ourClient;
    private final URI ourUri;

    private final String src;


    public GateStatusClient(String hostName, int hostPort, String Path){
        ourClient = HttpClient.newHttpClient();
        try {
            ourUri = new URI("http", null, hostName, hostPort, Path, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        this.src = Path;
    }

    @Override
    public Client.Response call() {
        HttpRequest request = HttpRequest.newBuilder().uri(ourUri)
                .headers("Content-Type", "text/x-java-source")
                .GET().build();
        HttpResponse<String> httpRes;
        try {
            httpRes = ourClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        boolean cache = false;
        ourResponse = new Client.Response(httpRes.statusCode(), httpRes.body(), cache, src);
        return ourResponse;
    }
}
