package edu.yu.cs.com3800.stage5;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


public class TestClient implements Client, Callable<Client.Response> {
    private Response ourResponse;
    private final HttpClient ourClient;
    private final URI ourUri;
    private final String src;


    public TestClient(String hostName, int hostPort, String src){
        ourClient = HttpClient.newHttpClient();
        try {
            ourUri = new URI("http", null, hostName, 8888, "/compileandrun", null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        this.src = src;
    }

    @Override
    public Response call() {
        if(src == null){
            throw new IllegalArgumentException("src is null");
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(ourUri)
                .headers("Content-Type", "text/x-java-source")
                .timeout(Duration.ofSeconds(120))
                .POST(HttpRequest.BodyPublishers.ofString(src))
                .build();
        HttpResponse<String> httpRes;
        try {
            httpRes = ourClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            return null;
        }
        Map<String, List<String>> headers = httpRes.headers().map();
        boolean cache;
        if(headers.get("Cached-Response").getFirst().equals("true")){
            cache = true;
        }else {
            cache = false;
        }
        ourResponse = new Response(httpRes.statusCode(), httpRes.body(), cache, src);
        return ourResponse;
    }
}
