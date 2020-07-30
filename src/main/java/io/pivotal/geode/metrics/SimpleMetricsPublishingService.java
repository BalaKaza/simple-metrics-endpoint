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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
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
    private final Map<String, Map<String, Number>> serverMetrics;
    private MetricsSession session;
    private PrometheusMeterRegistry registry;
    private HttpServer server;

    public SimpleMetricsPublishingService() {
        this(PORT);
    }

    public SimpleMetricsPublishingService(int port) {
        this.port = port;
        this.serverMetrics = new ConcurrentHashMap<>();
        this.executor = LoggingExecutors.newSingleThreadScheduledExecutor("MetricsProvider");
    }

    @Override
    public void start(MetricsSession session) {
        this.session = session;
        registry = new PrometheusMeterRegistry(DEFAULT);
        this.session.addSubregistry(registry);
        this.executor.scheduleAtFixedRate(() -> updateMetrics(), 2000, 2000, TimeUnit.MILLISECONDS);
        try {
            HOSTNAME = InetAddress.getLocalHost().getHostName();
            LOG.info(HOSTNAME);
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
            List<Metric> allMetrics = new ArrayList<>();
            StatisticsManager statisticsManager = system.getStatisticsManager();
            for (Statistics statistics : statisticsManager.getStatsList()) {
                StatisticsType statisticsType = statistics.getType();
                for (StatisticDescriptor descriptor : statisticsType.getStatistics()) {
                    String statName = descriptor.getName();
//                    LOG.info("SimpleMetricsPublishingService.updateMetrics processing statName=" + statName + "statValue=" + statistics.get(statName));
                    Metric metric = new Metric("geode_" + statName, statistics.get(statName), statisticsType.getName(), statistics.getTextId());
                    allMetrics.add(metric);
                }
            }
            for (Metric metric : allMetrics) {
                String hostName = system.getMemberId();
                updateServerMetric(hostName, metric);
            }
        }
    }

    private void updateServerMetric(String serverName, Metric metric) {
        Map<String, Number> metrics = this.serverMetrics.get(serverName);
        if (metrics == null) {
            createGauge(serverName, metric);
        } else {
            Number currentValue = metrics.get(metric.getMapName());
            if (currentValue == null) {
                createGauge(serverName, metric);
            } else {
                Number newValue = metric.getValue();
                if (newValue instanceof Integer && currentValue instanceof AtomicInteger) {
                    AtomicInteger ai = (AtomicInteger) currentValue;
                    Integer i = (Integer) newValue;
                    ai.set(i);
                } else if (newValue instanceof Long && currentValue instanceof AtomicLong) {
                    AtomicLong al = (AtomicLong) currentValue;
                    Long l = (Long) newValue;
                    al.set(l);
                } else if (newValue instanceof Double && currentValue instanceof AtomicLong) {
                    AtomicLong al = (AtomicLong) currentValue;
                    Long l = Double.doubleToLongBits((Double) newValue);
                    al.set(l);
                }
            }
        }
    }

    protected void createGauge(String serverName, Metric metric) {
        Tags tags = Tags
                .of("member", serverName)
                .and("category", metric.getCategory())
                .and("type", metric.getType());
        Gauge
                .builder(metric.getCategory()+metric.getName(), this, provider -> provider.getServerMetric(serverName, metric))
                .tags(tags)
                .register(this.registry);
        addServerMetric(serverName, metric);
        LOG.info("Registered gauge for server={}; category={}; type={}; metric={}", serverName, metric.getCategory(), metric.getType(), metric.getName());
    }

    private void addServerMetric(String serverName, Metric metric, Number metricValue) {
        Map<String, Number> metrics = this.serverMetrics.get(serverName);
        if (metrics == null) {
            metrics = new ConcurrentHashMap<>();
            this.serverMetrics.put(serverName, metrics);
            LOG.info("Created metrics map for server={}", serverName);
        }
        metrics.put(metric.getMapName(), metricValue);
    }

    protected void addServerMetric(String serverName, Metric metric) {
        Number metricCounter = 0;
        if (metric.getValue() instanceof Integer) {
            metricCounter = new AtomicInteger((Integer) metric.getValue());
        } else if (metric.getValue() instanceof Long) {
            metricCounter = new AtomicLong((Long) metric.getValue());
        } else if (metric.getValue() instanceof Double) {
            long metricValueL = Double.doubleToLongBits((Double) metric.getValue());
            metricCounter = new AtomicLong(metricValueL);
        }
        addServerMetric(serverName, metric, metricCounter);
    }

    protected double getServerMetric(String serverName, Metric metric) {
        double currentValue = 0;
        Map<String, Number> metrics = this.serverMetrics.get(serverName);
        if (metrics == null) {
            LOG.warn("Metric server={}; category={}; type={}; metric={} cannot be retrieved since the server does not exist", serverName, metric.getCategory(), metric.getType(), metric.getName());
        } else {
            Number currentAtomicValue = metrics.get(metric.getMapName());
            if (currentAtomicValue == null) {
                LOG.warn("Metric server={}; category={}; type={}; metric={} cannot be retrieved since the metric does not exist", serverName, metric.getCategory(), metric.getType(), metric.getName());
            } else {
                if (currentAtomicValue instanceof AtomicInteger) {
                    AtomicInteger ai = (AtomicInteger) currentAtomicValue;
                    currentValue = ai.get();
                } else if (currentAtomicValue instanceof AtomicLong && metric.getValue() instanceof Long) {
                    AtomicLong al = (AtomicLong) currentAtomicValue;
                    currentValue = al.get();
                } else if (currentAtomicValue instanceof AtomicLong && metric.getValue() instanceof Double) {
                    AtomicLong al = (AtomicLong) currentAtomicValue;
                    currentValue = Double.longBitsToDouble(al.get());
                }
            }
        }
        return currentValue;
    }

    private void requestHandler(HttpExchange httpExchange) throws IOException {
        final byte[] scrapeBytes = registry.scrape().getBytes();
        httpExchange.sendResponseHeaders(200, scrapeBytes.length);
        final OutputStream responseBody = httpExchange.getResponseBody();
        responseBody.write(scrapeBytes);
        responseBody.close();
    }

    @Override
    public void stop(MetricsSession session) {
        session.removeSubregistry(registry);
        registry = null;
        server.stop(0);
    }
}
