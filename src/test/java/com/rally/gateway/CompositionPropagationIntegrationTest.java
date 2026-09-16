package com.rally.gateway;

import io.jsonwebtoken.Jwts;
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
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies correlation propagation on every composition fan-out path: each secondary
 * {@code WebClient} call must carry the same {@code X-Correlation-Id} the gateway
 * generated for the incoming request, plus a valid W3C {@code traceparent}.
 *
 * <p>Five stub downstreams stand in for catalog/deal/auth/participation/order. Each
 * records the headers it received (keyed {@code stubName + " " + path}) and returns the
 * minimal JSON its caller needs to proceed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class CompositionPropagationIntegrationTest {

    private static final Pattern TRACEPARENT =
            Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]");
    private static final KeyPair KEY_PAIR = generateKeyPair();

    /** stubName + " " + path → headers received (header name → joined values). */
    private static final Map<String, List<Map<String, String>>> RECEIVED = new ConcurrentHashMap<>();

    private static final DisposableServer CATALOG = stub("catalog", CompositionPropagationIntegrationTest::catalogResponse);
    private static final DisposableServer DEAL = stub("deal", CompositionPropagationIntegrationTest::dealResponse);
    private static final DisposableServer AUTH = stub("auth", CompositionPropagationIntegrationTest::authResponse);
    private static final DisposableServer PARTICIPATION = stub("participation", CompositionPropagationIntegrationTest::participationResponse);
    private static final DisposableServer ORDER = stub("order", (path, query) -> "{\"total\":3}");

    @DynamicPropertySource
    static void stubUris(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.gateway.server.webflux.default-filters[0]",
                () -> "RemoveRequestHeader=Authorization");
        // Replace the YAML route list with one inert route; the Java-DSL composition
        // routes under test still load and match first for their paths.
        registry.add("spring.cloud.gateway.server.webflux.routes[0].id", () -> "unused");
        registry.add("spring.cloud.gateway.server.webflux.routes[0].uri", () -> "http://localhost:9");
        registry.add("spring.cloud.gateway.server.webflux.routes[0].predicates[0]", () -> "Path=/unused/**");

        registry.add("CATALOG_SERVICE_URI", () -> "http://localhost:" + CATALOG.port());
        registry.add("DEAL_SERVICE_URI", () -> "http://localhost:" + DEAL.port());
        registry.add("AUTH_SERVICE_URI", () -> "http://localhost:" + AUTH.port());
        registry.add("PARTICIPATION_SERVICE_URI", () -> "http://localhost:" + PARTICIPATION.port());
        registry.add("ORDER_SERVICE_URI", () -> "http://localhost:" + ORDER.port());

        // All composition paths are public here so no JWT is needed except /profile/**,
        // which requires X-User-Id (minted tokens are used for those tests instead).
        registry.add("rally.gateway.public-paths",
                () -> "/deals/**,/products/**,/users/**,/participations/**,/auth/**,/api/orders/**,/uploads/**");
        registry.add("rally.jwt.public-key", () -> toPem(KEY_PAIR.getPublic()));
    }

    @LocalServerPort
    private int port;

    @Autowired
    private WebTestClient webTestClient;

    private WebTestClient realClient;

    @BeforeEach
    void bindRealClient() {
        RECEIVED.clear();
        realClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    @AfterAll
    static void stopStubs() {
        CATALOG.disposeNow();
        DEAL.disposeNow();
        AUTH.disposeNow();
        PARTICIPATION.disposeNow();
        ORDER.disposeNow();
    }

    @Test
    void dealListEnrichmentCarriesCorrelationToCatalog() {
        realClient.get().uri("/deals")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.content[0].productName").isEqualTo("N1");

        // Primary proxy (deal) and secondary batch call (catalog) share one id + trace.
        String primary = headerOf("deal /deals", "X-Correlation-Id");
        String secondary = headerOf("catalog /internal/products/batch", "X-Correlation-Id");
        assertThat(UUID.fromString(primary)).isNotNull();
        assertThat(secondary).isEqualTo(primary);
        assertThat(headerOf("catalog /internal/products/batch", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("catalog /internal/products/batch", "traceparent"))
                .isEqualTo(headerOf("deal /deals", "traceparent"));
    }

    @Test
    void productListEnrichmentCarriesCorrelationToDeal() {
        realClient.get().uri("/products")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items[0].deals").isArray();

        String primary = headerOf("catalog /products", "X-Correlation-Id");
        String secondary = headerOf("deal /deals", "X-Correlation-Id");
        assertThat(UUID.fromString(primary)).isNotNull();
        assertThat(secondary).isEqualTo(primary);
        assertThat(headerOf("deal /deals", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("deal /deals", "traceparent"))
                .isEqualTo(headerOf("catalog /products", "traceparent"));
    }

    @Test
    void sellerEnrichmentCarriesCorrelationToCatalog() {
        realClient.get().uri("/users/sellers")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items[0].totalProducts").isEqualTo(2);

        String primary = headerOf("auth /users/sellers", "X-Correlation-Id");
        String secondary = headerOf("catalog /internal/products/sellers-summary", "X-Correlation-Id");
        assertThat(UUID.fromString(primary)).isNotNull();
        assertThat(secondary).isEqualTo(primary);
        assertThat(headerOf("catalog /internal/products/sellers-summary", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("catalog /internal/products/sellers-summary", "traceparent"))
                .isEqualTo(headerOf("auth /users/sellers", "traceparent"));
    }

    @Test
    void participantEnrichmentCarriesCorrelationToAuth() {
        realClient.get().uri("/deals/d1/participants")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.participants[0].firstName").isEqualTo("F");

        String primary = headerOf("participation /deals/d1/participants", "X-Correlation-Id");
        String secondary = headerOf("auth /users/batch", "X-Correlation-Id");
        assertThat(UUID.fromString(primary)).isNotNull();
        assertThat(secondary).isEqualTo(primary);
        assertThat(headerOf("auth /users/batch", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("auth /users/batch", "traceparent"))
                .isEqualTo(headerOf("participation /deals/d1/participants", "traceparent"));
    }

    @Test
    void profilePersonalInfoFanOutSharesOneCorrelationId() {
        webTestClient.get().uri("/profile/personal-info")
                .header("Authorization", "Bearer " + signToken("user-1", "SELLER"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.ordersCount").isEqualTo(3);

        String auth = headerOf("auth /auth/me", "X-Correlation-Id");
        String participations = headerOf("participation /participations", "X-Correlation-Id");
        String orders = headerOf("order /api/orders/my", "X-Correlation-Id");
        assertThat(UUID.fromString(auth)).isNotNull();
        assertThat(participations).isEqualTo(auth);
        assertThat(orders).isEqualTo(auth);
        assertThat(headerOf("auth /auth/me", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("participation /participations", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("order /api/orders/my", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("participation /participations", "traceparent"))
                .isEqualTo(headerOf("auth /auth/me", "traceparent"));
        assertThat(headerOf("order /api/orders/my", "traceparent"))
                .isEqualTo(headerOf("auth /auth/me", "traceparent"));
    }

    @Test
    void profileMyDealsFanOutSharesOneCorrelationId() {
        webTestClient.get().uri("/profile/my-deals?page=1&size=20")
                .header("Authorization", "Bearer " + signToken("user-1", "SELLER"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.deals[0].productName").isEqualTo("N1");

        String participations = headerOf("participation /participations", "X-Correlation-Id");
        String bulk = headerOf("deal /deals/bulk", "X-Correlation-Id");
        String products = headerOf("catalog /internal/products/batch", "X-Correlation-Id");
        assertThat(UUID.fromString(participations)).isNotNull();
        assertThat(bulk).isEqualTo(participations);
        assertThat(products).isEqualTo(participations);
        assertThat(headerOf("deal /deals/bulk", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("catalog /internal/products/batch", "traceparent")).matches(TRACEPARENT);
        assertThat(headerOf("deal /deals/bulk", "traceparent"))
                .isEqualTo(headerOf("participation /participations", "traceparent"));
        assertThat(headerOf("catalog /internal/products/batch", "traceparent"))
                .isEqualTo(headerOf("participation /participations", "traceparent"));
    }

    @Test
    void proxiedComposedResponseEchoesIds() {
        var result = realClient.get()
                .uri("/deals")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .returnResult();

        assertThat(UUID.fromString(result.getResponseHeaders().getFirst("X-Correlation-Id"))).isNotNull();
        assertThat(result.getResponseHeaders().getFirst("X-Correlation-Id"))
                .isEqualTo(headerOf("deal /deals", "X-Correlation-Id"));
        assertThat(result.getResponseHeaders().getFirst("traceparent")).matches(TRACEPARENT);
    }

    @Test
    void shortCircuitedProfileResponseEchoesIds() {
        var result = webTestClient.get().uri("/profile/personal-info")
                .header("Authorization", "Bearer " + signToken("user-1", "SELLER"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .returnResult();

        assertThat(UUID.fromString(result.getResponseHeaders().getFirst("X-Correlation-Id"))).isNotNull();
        assertThat(result.getResponseHeaders().getFirst("X-Correlation-Id"))
                .isEqualTo(headerOf("auth /auth/me", "X-Correlation-Id"));
        assertThat(result.getResponseHeaders().getFirst("traceparent")).matches(TRACEPARENT);
    }

    // --- stub bodies ---

    private static String catalogResponse(String path, String query) {
        return switch (path) {
            case "/products" -> "{\"items\":[{\"id\":\"p1\"}]}";
            case "/internal/products/batch" ->
                    "[{\"id\":\"p1\",\"productName\":\"N1\",\"productImageUrl\":\"u\",\"category\":{\"name\":\"C\"},\"sku\":\"S\",\"sellerName\":\"SN\"}]";
            case "/internal/products/sellers-summary" ->
                    "{\"sellers\":[{\"sellerId\":\"s1\",\"totalProducts\":2,\"totalPendingProducts\":1}]}";
            default -> "{}";
        };
    }

    private static String dealResponse(String path, String query) {
        return switch (path) {
            case "/deals" -> query.contains("productId")
                    ? "{\"content\":[{\"id\":\"d1\"}]}"
                    : "{\"content\":[{\"id\":\"d1\",\"productId\":\"p1\",\"status\":\"ACTIVE\"}]}";
            case "/deals/bulk" ->
                    "[{\"id\":\"d1\",\"status\":\"ACTIVE\",\"productId\":\"p1\",\"originalPrice\":100,\"dealPrice\":80}]";
            default -> "{}";
        };
    }

    private static String authResponse(String path, String query) {
        return switch (path) {
            case "/users/sellers" -> "{\"items\":[{\"id\":\"s1\"}]}";
            case "/users/batch" ->
                    "[{\"id\":\"u1\",\"firstName\":\"F\",\"lastName\":\"L\",\"email\":\"E\"}]";
            case "/auth/me" -> "{\"id\":\"u1\"}";
            default -> "{}";
        };
    }

    private static String participationResponse(String path, String query) {
        return switch (path) {
            case "/participations" ->
                    "{\"participations\":[{\"dealId\":\"d1\",\"status\":\"ACTIVE\"}],\"totalElements\":1}";
            default -> "{\"participants\":[{\"userId\":\"u1\"}]}";
        };
    }

    // --- helpers ---

    private interface Responder {
        String respond(String path, String query);
    }

    private static DisposableServer stub(String name, Responder responder) {
        return HttpServer.create()
                .port(0)
                .handle((request, response) -> request.receive()
                        .aggregate()
                        .asString()
                        .defaultIfEmpty("")
                        .flatMap(ignored -> {
                            String uri = request.uri();
                            int query = uri.indexOf('?');
                            String path = query >= 0 ? uri.substring(0, query) : uri;
                            String queryString = query >= 0 ? uri.substring(query + 1) : "";
                            RECEIVED.computeIfAbsent(name + " " + path, k -> new CopyOnWriteArrayList<>())
                                    .add(collectHeaders(request.requestHeaders()));
                            return response
                                    .header(HttpHeaders.CONTENT_TYPE, "application/json")
                                    .sendString(Mono.just(responder.respond(path, queryString)))
                                    .then();
                        }))
                .bindNow();
    }

    private static Map<String, String> collectHeaders(io.netty.handler.codec.http.HttpHeaders headers) {
        Map<String, String> result = new java.util.LinkedHashMap<>();
        headers.forEach(entry -> result.merge(entry.getKey(), entry.getValue(), (a, b) -> a + "," + b));
        return result;
    }

    private static String headerOf(String key, String header) {
        List<Map<String, String>> calls = RECEIVED.get(key);
        assertThat(calls).as("expected downstream call %s", key).isNotNull().isNotEmpty();
        Map<String, String> headers = calls.get(calls.size() - 1);
        String value = headers.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(header))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
        assertThat(value).as("header %s on %s, got %s", header, key, headers).isNotBlank();
        return value;
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

    private static String signToken(String userId, String role) {
        Instant now = Instant.now();
        return Jwts.builder()
                .subject(userId)
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plusSeconds(900)))
                .signWith(KEY_PAIR.getPrivate(), Jwts.SIG.RS256)
                .compact();
    }
}
