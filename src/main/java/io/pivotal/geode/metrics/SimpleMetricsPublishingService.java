package io.pivotal.geode.metrics;

import static io.micrometer.prometheus.PrometheusConfig.DEFAULT;
import static java.lang.Integer.getInteger;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.slf4j.Logger;

import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.metrics.MetricsSession;

public class SimpleMetricsPublishingService implements MetricsPublishingService {
  private static final String PORT_PROPERTY = "prometheus.metrics.mport";
  private static final int DEFAULT_PORT = 0; // If no port specified, use any port
  private static String HOSTNAME = "localhost";

  private static final int PORT = getInteger(PORT_PROPERTY, DEFAULT_PORT);
  private final ScheduledExecutorService executor;

  private static Logger LOG = getLogger(SimpleMetricsPublishingService.class);

  private final int port;
  private InternalDistributedSystem system;
  private MetricsSession session;
  private PrometheusMeterRegistry registry;
  private HttpServer server;

  public SimpleMetricsPublishingService() {
    this(PORT);
  }

  public SimpleMetricsPublishingService(int port) {
    this.port = port;
    this.executor = LoggingExecutors.newSingleThreadScheduledExecutor("MetricsProvider");
  }

  @Override
  public void start(MetricsSession session) {
    this.session = session;
    registry = new PrometheusMeterRegistry(DEFAULT);
    this.session.addSubregistry(registry);
    LOG.info("Here is the Cache Name:" + CacheFactory.getAnyInstance().getName());
    LOG.info("SimpleMetricsPublishingService.start session=" + session);
    this.executor.scheduleAtFixedRate(() -> updateMetrics(), 2000, 2000, TimeUnit.MILLISECONDS);
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

  private void updateMetrics() {
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    if (system == null) {
      LOG.error("SimpleMetricsPublishingService.updateMetrics distributed system is null");
    } else {
      StatisticsManager statisticsManager = system.getStatisticsManager();
      LOG.info("Statistics List Size:" + system.getStatisticsManager().getStatsList().size());
      for (Statistics statistics : statisticsManager.getStatsList()) {
        LOG.info("Statistics Type:" + statistics.getType());
        StatisticsType statisticsType = statistics.getType();
        for (StatisticDescriptor descriptor : statisticsType.getStatistics()) {
          LOG.info("Statistics Type:" + descriptor.getName());
          String statName = descriptor.getName();
          LOG.info("SimpleMetricsPublishingService.updateMetrics processing statName=" + statName + "statValue=" + statistics.get(statName));
        }
      }
    }
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
