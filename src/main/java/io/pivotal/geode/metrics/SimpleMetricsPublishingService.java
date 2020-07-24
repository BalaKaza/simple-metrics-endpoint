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
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.metrics.MetricsSession;

public class SimpleMetricsPublishingService implements MetricsPublishingService {
    private static final String PORT_PROPERTY = "prometheus.metrics.port";
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
        initializeMeterRegistry();
        initializeMetricsEndpoint();
    }

    @Override
    public void stop(MetricsSession session) {
        session.removeSubregistry(registry);
        registry = null;
        server.stop(0);
    }

    private void initializeMeterRegistry(){
        registry = new PrometheusMeterRegistry(DEFAULT);
        MeterFilter meterFilter = new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                return id.withName("geode." + id.getName());
            }
        };
        registry.config().meterFilter(meterFilter);
        session.addSubregistry(registry);
    }

    private void initializeMetricsEndpoint(){
        try {
            HOSTNAME = InetAddress.getLocalHost().getHostName();
            server = HttpServer.create(new InetSocketAddress(HOSTNAME, port), 0);
            HttpContext context = server.createContext("/");
            context.setHandler(this::requestHandler);
            server.start();
            LOG.info("Started {} http://{}:{}/", getClass().getSimpleName(), HOSTNAME,  server.getAddress().getPort());
        } catch (UnknownHostException e) {
            LOG.error("Unknown host exception while starting metrics endpoint.");
            e.printStackTrace();
        } catch (IOException thrown) {
            LOG.error("Exception while starting " + getClass().getSimpleName(), thrown);
        }
    }

    private void requestHandler(HttpExchange httpExchange) throws IOException {
        final byte[] scrapeBytes = registry.scrape().getBytes();
        httpExchange.sendResponseHeaders(200, scrapeBytes.length);
        final OutputStream responseBody = httpExchange.getResponseBody();
        responseBody.write(scrapeBytes);
        responseBody.close();
    }
}
