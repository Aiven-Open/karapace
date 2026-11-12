package poc.schemalinking;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Local HTTP proxy between the deserializer and the registry: it forwards every request as is and
 * records "METHOD path?query" (decoded query). It shows what the client really sends, without patching
 * the client or relying on the registry logs.
 */
final class RecordingProxy implements AutoCloseable {

  private final HttpServer server;
  private final HttpClient http = HttpClient.newHttpClient();
  private final String target;
  private final List<String> requests = new CopyOnWriteArrayList<>();

  RecordingProxy(String target) throws IOException {
    this.target = target.replaceAll("/+$", "");
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::forward);
    server.start();
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  List<String> requests() {
    return List.copyOf(requests);
  }

  void clear() {
    requests.clear();
  }

  private void forward(HttpExchange exchange) throws IOException {
    try (exchange) {
      URI uri = exchange.getRequestURI();
      String rawQuery = uri.getRawQuery();
      String query = rawQuery == null ? "" : "?" + URLDecoder.decode(rawQuery, StandardCharsets.UTF_8);
      requests.add(exchange.getRequestMethod() + " " + uri.getPath() + query);

      byte[] body = exchange.getRequestBody().readAllBytes();
      HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(target + uri.getRawPath()
              + (rawQuery == null ? "" : "?" + rawQuery)))
          .method(exchange.getRequestMethod(), body.length == 0
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofByteArray(body));
      for (String header : List.of("Content-Type", "Accept")) {
        String value = exchange.getRequestHeaders().getFirst(header);
        if (value != null) {
          request.header(header, value);
        }
      }

      HttpResponse<byte[]> response = http.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
      response.headers().firstValue("Content-Type")
          .ifPresent(v -> exchange.getResponseHeaders().add("Content-Type", v));
      byte[] responseBody = response.body();
      exchange.sendResponseHeaders(response.statusCode(), responseBody.length == 0 ? -1 : responseBody.length);
      if (responseBody.length > 0) {
        exchange.getResponseBody().write(responseBody);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
