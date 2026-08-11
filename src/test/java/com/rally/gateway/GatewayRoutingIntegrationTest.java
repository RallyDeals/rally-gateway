package com.rally.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rally.security.JwtService;
import org.junit.jupiter.api.AfterAll;
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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: a real stub downstream on a random port, routed through the gateway.
 * Proves the identity contract — valid JWT in, {@code X-User-Id}/{@code X-User-Role}
 * out, raw token and spoofed identity headers never reach the service, and bad/missing
 * tokens get a 401 in the rally-common error shape.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class GatewayRoutingIntegrationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final DisposableServer STUB = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                Map<String, List<String>> headers = collectHeaders(request.requestHeaders());
                return response
                        .header(HttpHeaders.CONTENT_TYPE, "application/json")
                        .sendString(Mono.just(toJson(pathOf(request), headers)));
            })
            .bindNow();

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void stubRoutes(DynamicPropertyRegistry registry) {
        // An indexed element cannot be mixed with the YAML routes list ("left unbound"),
        // so the whole list is re-declared here, mirroring the routes in application.yml
        // plus a stub route pointed at the random-port downstream.
        registry.add("spring.cloud.gateway.server.webflux.routes[0].id", () -> "auth");
        registry.add("spring.cloud.gateway.server.webflux.routes[0].uri", () -> "http://localhost:8084");
        registry.add("spring.cloud.gateway.server.webflux.routes[0].predicates[0]", () -> "Path=/auth/**,/users/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[1].id", () -> "catalog");
        registry.add("spring.cloud.gateway.server.webflux.routes[1].uri", () -> "http://localhost:8083");
        registry.add("spring.cloud.gateway.server.webflux.routes[1].predicates[0]", () -> "Path=/products/**,/categories/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[2].id", () -> "participation");
        registry.add("spring.cloud.gateway.server.webflux.routes[2].uri", () -> "http://localhost:8086");
        registry.add("spring.cloud.gateway.server.webflux.routes[2].predicates[0]",
                () -> "Path=/deals/*/join,/deals/*/leave,/deals/*/participants,/deals/*/progress,/deals/*/invite-link,/invites/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[3].id", () -> "deal");
        registry.add("spring.cloud.gateway.server.webflux.routes[3].uri", () -> "http://localhost:8085");
        registry.add("spring.cloud.gateway.server.webflux.routes[3].predicates[0]", () -> "Path=/deals/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[4].id", () -> "order");
        registry.add("spring.cloud.gateway.server.webflux.routes[4].uri", () -> "http://localhost:8081");
        registry.add("spring.cloud.gateway.server.webflux.routes[4].predicates[0]", () -> "Path=/api/orders/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[5].id", () -> "payment");
        registry.add("spring.cloud.gateway.server.webflux.routes[5].uri", () -> "http://localhost:8082");
        registry.add("spring.cloud.gateway.server.webflux.routes[5].predicates[0]",
                () -> "Path=/api/payments/**,/api/users/*/payment-methods/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[6].id", () -> "inventory");
        registry.add("spring.cloud.gateway.server.webflux.routes[6].uri", () -> "http://localhost:8087");
        registry.add("spring.cloud.gateway.server.webflux.routes[6].predicates[0]", () -> "Path=/inventory/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[7].id", () -> "notification");
        registry.add("spring.cloud.gateway.server.webflux.routes[7].uri", () -> "http://localhost:8088");
        registry.add("spring.cloud.gateway.server.webflux.routes[7].predicates[0]", () -> "Path=/notifications/**");

        registry.add("spring.cloud.gateway.server.webflux.routes[8].id", () -> "stub");
        registry.add("spring.cloud.gateway.server.webflux.routes[8].uri", () -> "http://localhost:" + STUB.port());
        registry.add("spring.cloud.gateway.server.webflux.routes[8].predicates[0]", () -> "Path=/stub/**");

        registry.add("rally.gateway.public-paths",
                () -> "/auth/login,/auth/register,/auth/refresh,/stub/public");
    }

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private JwtService jwtService;

    @AfterAll
    static void stopStub() {
        STUB.disposeNow();
    }

    @Test
    void validTokenIsReplacedByIdentityHeadersDownstream() {
        String token = jwtService.generateAccessToken("user-1", List.of("SELLER"));

        webTestClient.get()
                .uri("/stub/echo")
                .header("Authorization", "Bearer " + token)
                .header("X-User-Id", "spoofed")
                .header("X-User-Role", "ADMIN")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.path").isEqualTo("/stub/echo")
                .jsonPath("$.headers.X-User-Id").isEqualTo("user-1")
                .jsonPath("$.headers.X-User-Role").isEqualTo("SELLER")
                .jsonPath("$.headers.Authorization").doesNotExist();
    }

    @Test
    void missingTokenIsRejectedWithRallyErrorShape() {
        webTestClient.get()
                .uri("/stub/echo")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectBody()
                .jsonPath("$.status").isEqualTo(401)
                .jsonPath("$.title").isEqualTo("Unauthenticated")
                .jsonPath("$.message").isEqualTo("Missing bearer token")
                .jsonPath("$.path").isEqualTo("/stub/echo");
    }

    @Test
    void invalidTokenIsRejectedWith401() {
        webTestClient.get()
                .uri("/stub/echo")
                .header("Authorization", "Bearer garbage")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectBody()
                .jsonPath("$.title").isEqualTo("Unauthenticated");
    }

    @Test
    void publicPathForwardsEvenWithGarbageToken() {
        webTestClient.get()
                .uri("/stub/public/hello")
                .header("Authorization", "Bearer garbage")
                .header("X-User-Id", "spoofed")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.path").isEqualTo("/stub/public/hello")
                .jsonPath("$.headers.Authorization").doesNotExist()
                .jsonPath("$.headers.X-User-Id").doesNotExist();
    }

    /**
     * Browser-like preflight over real HTTP (WebTestClient's default client is mock-based
     * and sends a relative request URI, which Spring's same-origin check rejects before CORS
     * is even evaluated — a real browser always sends a Host header and an absolute request
     * URI). The gateway's globalcors must answer it with 200 and the allowed origin.
     */
    @Test
    void corsPreflightIsAnsweredByTheGateway() {
        WebTestClient realClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();

        var result = realClient.options()
                .uri("/stub/echo")
                .header(HttpHeaders.ORIGIN, "http://localhost:5173")
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .exchange()
                .returnResult(String.class);

        assertThat(result.getStatus().value()).isEqualTo(200);
        assertThat(result.getResponseHeaders().getFirst(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN))
                .isEqualTo("http://localhost:5173");
        assertThat(result.getResponseHeaders().getFirst(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS))
                .isEqualTo("true");
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
        headers.forEach(entry -> result.computeIfAbsent(entry.getKey(), k -> new java.util.ArrayList<>()).add(entry.getValue()));
        return result;
    }
}
