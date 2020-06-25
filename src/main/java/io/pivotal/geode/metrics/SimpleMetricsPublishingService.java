package io.pivotal.geode.metrics;

import static io.micrometer.prometheus.PrometheusConfig.DEFAULT;
import static java.lang.Integer.getInteger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.http.conn.util.InetAddressUtils;
import org.slf4j.Logger;

import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.metrics.MetricsSession;

public class SimpleMetricsPublishingService implements MetricsPublishingService {
  private static final String PORT_PROPERTY = "prometheus.metrics.mport";
  private static final int DEFAULT_PORT = 0; // If no port specified, use any port
  private static String HOSTNAME = "localhost";

  private static final int PORT = getInteger(PORT_PROPERTY, DEFAULT_PORT);

  private static Logger LOG = getLogger(SimpleMetricsPublishingService.class);

  private final int port;
  private MetricsSession session;
  private PrometheusMeterRegistry registry;
  private HttpServer server;

  public SimpleMetricsPublishingService() {
    this(PORT);
  }

  public SimpleMetricsPublishingService(int port) {
    this.port = port;
  }

  @Override
  public void start(MetricsSession session) {
    this.session = session;
    registry = new PrometheusMeterRegistry(DEFAULT);
    this.session.addSubregistry(registry);
    try {
      HOSTNAME = InetAddress.getLocalHost().getHostName();
      System.out.println(HOSTNAME);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    InetSocketAddress address = new InetSocketAddress(HOSTNAME, port);
    server = null;
    try {
      server = HttpServer.create(address, 0);
    } catch (IOException thrown) {
      LOG.error("Exception while starting " + getClass().getSimpleName(), thrown);
    }
    HttpContext context = server.createContext("/");
    context.setHandler(this::requestHandler);
    server.start();

    int boundPort = server.getAddress().getPort();
    LOG.info("Started {} http://{}:{}/", getClass().getSimpleName(), HOSTNAME, boundPort);
  }

  private void requestHandler(HttpExchange httpExchange) throws IOException {
    final byte[] scrapeBytes = registry.scrape().getBytes();
    httpExchange.sendResponseHeaders(200, scrapeBytes.length);
    final OutputStream responseBody = httpExchange.getResponseBody();
    responseBody.write(scrapeBytes);
    responseBody.close();
  }

  @Override
  public void stop() {
    session.removeSubregistry(registry);
    registry = null;
    server.stop(0);
  }
}
