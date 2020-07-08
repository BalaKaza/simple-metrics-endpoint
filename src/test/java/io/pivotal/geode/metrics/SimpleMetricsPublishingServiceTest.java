package io.pivotal.geode.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.metrics.MetricsSession;

@ExtendWith(MockitoExtension.class)
class SimpleMetricsPublishingServiceTest {

//  @Mock
//  private MetricsSession metricsSession;
//
//  private MetricsPublishingService subject;
//  private String HOSTNAME;
//
//  @BeforeEach
//  void setUp() throws UnknownHostException {
//    subject = new SimpleMetricsPublishingService(9000);
//    HOSTNAME = InetAddress.getLocalHost().getHostName();
//  }
//
//  @Test
//  void start_addsRegistryToMetricsSession() {
//    subject.start(metricsSession);
//
//    verify(metricsSession).addSubregistry(any(PrometheusMeterRegistry.class));
//
//    subject.stop();
//  }
//
//  @Test
//  void start_addsAnHttpEndpointThatReturnsStatusOK() throws IOException {
//    subject.start(metricsSession);
//
//    HttpGet request = new HttpGet("http://" + HOSTNAME + ":9000/");
//    HttpResponse response = HttpClientBuilder.create().build().execute(request);
//
//    assertThat(response.getStatusLine().getStatusCode())
//        .isEqualTo(HttpStatus.SC_OK);
//
//    subject.stop();
//  }
//
//  @Test
//  void start_addsAnHttpEndpointThatContainsRegistryData() throws IOException {
//    subject.start(metricsSession);
//
//    HttpGet request = new HttpGet("http://" + HOSTNAME + ":9000/");
//    HttpResponse response = HttpClientBuilder.create().build().execute(request);
//
//    String responseBody = EntityUtils.toString(response.getEntity());
//    assertThat(responseBody).isEmpty();
//
//    subject.stop();
//  }
//
//  @Test
//  void stop_removesRegistryFromMetricsSession() {
//    subject.start(metricsSession);
//    subject.stop();
//
//    verify(metricsSession).removeSubregistry(any(PrometheusMeterRegistry.class));
//  }
//
//  @Test
//  void stop_hasNoHttpEndpointRunning() {
//    subject.start(metricsSession);
//    subject.stop();
//
//    HttpGet request = new HttpGet("http://" + HOSTNAME + ":9000/metrics");
//
//    assertThrows(HttpHostConnectException.class, () -> {
//      HttpClientBuilder.create().build().execute(request);
//    });
//  }
}