package com.rally.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the observability contract end-to-end through a stub downstream:
 * <ul>
 *   <li>{@code X-Correlation-Id} is gateway-owned — always present downstream as a fresh
 *       UUID, client-supplied values are replaced, ids are unique per request.</li>
 *   <li>W3C {@code traceparent} is Micrometer-owned — a client-sent value is propagated
 *       downstream untouched, and one is auto-generated when the client sends none.</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class CorrelationTraceIntegrationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final KeyPair KEY_PAIR = generateKeyPair();

    private static final Pattern TRACEPARENT =
            Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]");
    private static final String CLIENT_TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private static final DisposableServer STUB = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                Map<String, List<String>> headers = collectHeaders(request.requestHeaders());
                return response
                        .header(HttpHeaders.CONTENT_TYPE, "application/json")
                        .sendString(Mono.just(toJson(pathOf(request), headers)));
            })
            .bindNow();

    @DynamicPropertySource
    static void stubRoutes(DynamicPropertyRegistry registry) {
        // Drop the Redis-backed RequestRateLimiter so tests stay hermetic (same as
        // GatewayRoutingIntegrationTest); keep only the Authorization strip safety net.
        registry.add("spring.cloud.gateway.server.webflux.default-filters[0]",
                () -> "RemoveRequestHeader=Authorization");

        // Single stub route. Declaring routes[0] replaces the YAML list for this context —
        // fine here because composition routes (Java DSL) still load and /stub/** is unique.
        registry.add("spring.cloud.gateway.server.webflux.routes[0].id", () -> "stub");
        registry.add("spring.cloud.gateway.server.webflux.routes[0].uri",
                () -> "http://localhost:" + STUB.port());
        registry.add("spring.cloud.gateway.server.webflux.routes[0].predicates[0]",
                () -> "Path=/stub/**");

        registry.add("rally.gateway.public-paths", () -> "/stub/public/**");
        registry.add("rally.jwt.public-key", () -> toPem(KEY_PAIR.getPublic()));
    }

    @Autowired
    private WebTestClient webTestClient;

    @LocalServerPort
    private int port;

    private WebTestClient realClient;

    @Autowired(required = false)
    private Tracer tracer;

    @Autowired(required = false)
    private ObservationRegistry observationRegistry;

    @Autowired
    private org.springframework.core.env.Environment environment;

    @Test
    void gatewayNativeObservationStaysDisabled() {
        // Single trace ownership: the gateway's own filters issue the traceparent.
        // The native http.client.requests observation would inject a competing one
        // downstream (forking a second trace per request) while the response echo
        // carries ours — see application.yml spring.cloud.gateway.observability.
        assertThat(environment.getProperty("spring.cloud.gateway.server.webflux.observability.enabled"))
                .isEqualTo("false");
    }

    @Test
    void issuedTraceparentIsOtelParseable() {
        // Guards the generator format against the real OTel parser (not just regex).
        java.util.Map<String, String> carrier = java.util.Map.of("traceparent", CLIENT_TRACEPARENT);
        io.opentelemetry.context.Context extracted =
                io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance()
                        .extract(io.opentelemetry.context.Context.root(), carrier,
                                new io.opentelemetry.context.propagation.TextMapGetter<java.util.Map<String, String>>() {
                                    @Override
                                    public Iterable<String> keys(java.util.Map<String, String> c) {
                                        return c.keySet();
                                    }

                                    @Override
                                    public String get(java.util.Map<String, String> c, String k) {
                                        return c.get(k);
                                    }
                                });
        io.opentelemetry.api.trace.SpanContext parsed =
                io.opentelemetry.api.trace.Span.fromContext(extracted).getSpanContext();
        assertThat(parsed.isValid()).isTrue();
        assertThat(parsed.getTraceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(parsed.isSampled()).isTrue();
    }

    @BeforeEach
    void bindRealClient() {
        realClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    @AfterAll
    static void stopStub() {
        STUB.disposeNow();
    }

    @Test
    void forwardsFreshXCorrelationIdAsValidUuid() {
        String downstreamId = downstreamHeader("/stub/public/echo", "X-Correlation-Id");

        assertThat(downstreamId).isNotBlank();
        assertThat(UUID.fromString(downstreamId)).isNotNull();
    }

    @Test
    void replacesClientSuppliedXCorrelationId() {
        var result = webTestClient.get()
                .uri("/stub/public/echo")
                .header("X-Correlation-Id", "client-chosen-id")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .returnResult()
                .getResponseBody();

        assertThat(result).isNotNull();
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) result.get("headers");
        String downstreamId = headers.get("X-Correlation-Id");

        assertThat(downstreamId).isNotEqualTo("client-chosen-id");
        assertThat(UUID.fromString(downstreamId)).isNotNull();
    }

    @Test
    void generatesUniqueCorrelationIdsPerRequest() {
        String first = downstreamHeader("/stub/public/echo", "X-Correlation-Id");
        String second = downstreamHeader("/stub/public/echo", "X-Correlation-Id");

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void tracingAutoConfigurationIsActive() {
        assertThat(tracer).as("Micrometer Tracer bean").isNotNull();
        assertThat(observationRegistry).as("ObservationRegistry bean").isNotNull();
    }

    @Test
    void replacesClientTraceparentDownstream() {
        // Zero-trust: even a valid client traceparent is replaced by the gateway-owned one.
        var result = realClient.get()
                .uri("/stub/public/echo")
                .header("traceparent", CLIENT_TRACEPARENT)
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .returnResult()
                .getResponseBody();

        assertThat(result).isNotNull();
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) result.get("headers");
        assertThat(headers.get("traceparent")).isNotEqualTo(CLIENT_TRACEPARENT);
        assertThat(headers.get("traceparent")).matches(TRACEPARENT);
    }

    @Test
    void generatesTraceparentWhenClientSendsNone() {
        String downstreamTraceparent = downstreamHeaderReal("/stub/public/echo", "traceparent");

        assertThat(downstreamTraceparent).matches(TRACEPARENT);
    }

    @Test
    void echoesSameIdsInResponseAsSentDownstream() {
        var result = realClient.get()
                .uri("/stub/public/echo")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .returnResult();

        assertThat(result.getResponseBody()).isNotNull();
        @SuppressWarnings("unchecked")
        Map<String, String> downstreamHeaders = (Map<String, String>) result.getResponseBody().get("headers");

        String responseCorrelationId = result.getResponseHeaders().getFirst("X-Correlation-Id");
        String responseTraceparent = result.getResponseHeaders().getFirst("traceparent");

        assertThat(UUID.fromString(responseCorrelationId)).isNotNull();
        assertThat(responseCorrelationId).isEqualTo(downstreamHeaders.get("X-Correlation-Id"));
        assertThat(responseTraceparent).matches(TRACEPARENT);
        assertThat(responseTraceparent).isEqualTo(downstreamHeaders.get("traceparent"));
    }

    // --- helpers ---

    private String downstreamHeader(String path, String header) {
        var result = webTestClient.get()
                .uri(path)
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .returnResult()
                .getResponseBody();

        assertThat(result).isNotNull();
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) result.get("headers");
        assertThat(headers).containsKey(header);
        return headers.get(header);
    }

    private String downstreamHeaderReal(String path, String header) {
        var result = realClient.get()
                .uri(path)
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .returnResult()
                .getResponseBody();

        assertThat(result).isNotNull();
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) result.get("headers");
        assertThat(headers).containsKey(header);
        return headers.get(header);
    }

    private static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String toPem(PublicKey key) {
        return "-----BEGIN PUBLIC KEY-----"
                + Base64.getEncoder().encodeToString(key.getEncoded())
                + "-----END PUBLIC KEY-----";
    }

    private static String pathOf(reactor.netty.http.server.HttpServerRequest request) {
        String uri = request.uri();
        int query = uri.indexOf('?');
        return query >= 0 ? uri.substring(0, query) : uri;
    }

    private static String toJson(String path, Map<String, List<String>> headers) {
        try {
            var root = OBJECT_MAPPER.createObjectNode();
            root.put("path", path);
            var headerNode = root.putObject("headers");
            headers.forEach((name, values) -> headerNode.put(name, String.join(",", values)));
            return OBJECT_MAPPER.writeValueAsString(root);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static Map<String, List<String>> collectHeaders(io.netty.handler.codec.http.HttpHeaders headers) {
        Map<String, List<String>> result = new java.util.LinkedHashMap<>();
        headers.forEach(entry -> result
                .computeIfAbsent(entry.getKey(), k -> new java.util.ArrayList<>())
                .add(entry.getValue()));
        return result;
    }
}
