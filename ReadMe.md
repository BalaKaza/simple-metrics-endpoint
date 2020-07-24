# Simple Geode Metrics Publishing Service

A simple service that publishes Geode metrics in Prometheus format.

## Build and Test
```bash
./gradlew build
```

This creates a jar file:
```
./build/libs/simple-metrics-endpoint.jar
```

Copy the jar file to the directory of your choice.

## Install

To add the publishing service to a Geode locator or server,
place the jar file on the classpath
when you launch the locator or server:

```
gfsh> start locator [...] --classpath=path-to-jar-file.jar
gfsh> start server [...] --classpath=path-to-jar-file.jar
```

Alternatively, you may copy the simple-metrics-endpoint.jar file
to the `extensions` directory in your Geode installation.

## View and Scrape the Metrics

By default, the publishing service starts its HTTP server
on any available port on `localhost`,
and logs the URL to the log file for the locator or server:

```
Started SimpleMetricsPublishingService http://localhost:<some-port-number>/
```

To specify a port,
set the `prometheus.metrics.port` system property
when you start the locator or server:
```
gfsh> start locator [...] --J=-Dprometheus.metrics.port=<my-port-number>
gfsh> start server [...] --J=-Dprometheus.metrics.port=<my-port-number>
```

Now you can configure your Prometheus server
to scrape metrics from that URL.
