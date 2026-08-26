package com.rally.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rally.security.JwtProperties;
import com.rally.security.JwtService;
import com.rally.gateway.config.GatewayProperties;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class JwtAuthGlobalFilterTest {

    private JwtService jwtService;
    private JwtAuthGlobalFilter filter;
    private PrivateKey privateKey;

    @BeforeEach
    void setUp() throws Exception {
        // This unit test does NOT need the production key pair — the filter logic only
        // cares that tokens are RS256-signed and validated against the matching public
        // key. Generate a fresh key pair at runtime so no key material exists in source.
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        privateKey = keyPair.getPrivate();

        JwtProperties jwtProperties = new JwtProperties();
        jwtProperties.setPublicKey(toPem(keyPair.getPublic()));
        jwtService = new JwtService(jwtProperties);

        GatewayProperties gatewayProperties = new GatewayProperties();
        // Mirror Spring Boot's auto-configured ObjectMapper (ErrorResponse carries an Instant).
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        filter = new JwtAuthGlobalFilter(jwtService, gatewayProperties, new ErrorResponseWriter(objectMapper));
    }

    @Test
    void validTokenInjectsIdentityHeadersAndStripsToken() {
        String token = signToken("user-1", "SELLER", Instant.now().plusSeconds(900));

        FilterOutcome outcome = runFilter(exchangeFor("/products",
                "Authorization", "Bearer " + token,
                "X-User-Id", "spoofed-user",
                "X-User-Role", "ADMIN"));

        assertThat(outcome.chained).isTrue();
        HttpHeaders headers = outcome.forwarded.getRequest().getHeaders();
        assertThat(headers.getFirst("X-User-Id")).isEqualTo("user-1");
        assertThat(headers.getFirst("X-User-Role")).isEqualTo("SELLER");
        assertThat(headers).doesNotContainKey("Authorization");
    }

    @Test
    void missingTokenOnProtectedPathReturns401() {
        FilterOutcome outcome = runFilter(exchangeFor("/products"));

        assertThat(outcome.chained).isFalse();
        assertThat(outcome.original.getResponse().getStatusCode().value()).isEqualTo(401);
        String body = outcome.original.getResponse().getBodyAsString().block();
        assertThat(body).contains("\"status\":401")
                .contains("\"title\":\"Unauthenticated\"")
                .contains("Missing bearer token");
    }

    @Test
    void malformedTokenOnProtectedPathReturns401() {
        FilterOutcome outcome = runFilter(exchangeFor("/products", "Authorization", "Bearer not-a-jwt"));

        assertThat(outcome.chained).isFalse();
        assertThat(outcome.original.getResponse().getStatusCode().value()).isEqualTo(401);
        assertThat(outcome.original.getResponse().getBodyAsString().block()).contains("Invalid token");
    }

    @Test
    void expiredTokenOnProtectedPathReturns401() {
        String expired = signToken("user-1", "SELLER", Instant.now().minusSeconds(30));

        FilterOutcome outcome = runFilter(exchangeFor("/products", "Authorization", "Bearer " + expired));

        assertThat(outcome.chained).isFalse();
        assertThat(outcome.original.getResponse().getStatusCode().value()).isEqualTo(401);
        assertThat(outcome.original.getResponse().getBodyAsString().block()).contains("Token expired");
    }

    @Test
    void publicPathForwardsWithoutTokenAndStripsClientIdentityHeaders() {
        FilterOutcome outcome = runFilter(exchangeFor("/auth/login",
                "Authorization", "Bearer garbage",
                "X-User-Id", "spoofed"));

        assertThat(outcome.chained).isTrue();
        HttpHeaders headers = outcome.forwarded.getRequest().getHeaders();
        assertThat(headers).doesNotContainKey("Authorization");
        assertThat(headers).doesNotContainKey("X-User-Id");
        assertThat(headers).doesNotContainKey("X-User-Role");
    }

    @Test
    void publicPathWithNoTokenForwards() {
        FilterOutcome outcome = runFilter(exchangeFor("/auth/register"));

        assertThat(outcome.chained).isTrue();
        assertThat(outcome.forwarded.getRequest().getHeaders()).doesNotContainKey("X-User-Id");
    }

    @Test
    void getDealsIsPublicButPostDealsStillRequiresAuthAndGetsUserIdInjected() {
        // Regression: application.yml's rally.gateway.public-paths used to list "/deals"
        // (meant only to make GET /deals public for anonymous browse), but that list is
        // matched by path only, with no method check — so it silently made POST /deals
        // public too, skipping JWT validation and X-User-Id injection on create. "/deals"
        // was removed from that list; GET /deals stays public only via isPublic()'s
        // explicit GET-only check below, and POST /deals must still require auth.
        FilterOutcome getOutcome = runFilter(exchangeFor("/deals"));
        assertThat(getOutcome.chained).isTrue();
        assertThat(getOutcome.forwarded.getRequest().getHeaders()).doesNotContainKey("X-User-Id");

        MockServerWebExchange postExchange = MockServerWebExchange.from(MockServerHttpRequest.post("/deals").build());
        FilterOutcome missingTokenOutcome = runFilter(postExchange);
        assertThat(missingTokenOutcome.chained).isFalse();
        assertThat(missingTokenOutcome.original.getResponse().getStatusCode().value()).isEqualTo(401);

        String token = signToken("user-1", "BUYER", Instant.now().plusSeconds(900));
        MockServerWebExchange authedPostExchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/deals").header("Authorization", "Bearer " + token).build());
        FilterOutcome authedOutcome = runFilter(authedPostExchange);
        assertThat(authedOutcome.chained).isTrue();
        assertThat(authedOutcome.forwarded.getRequest().getHeaders().getFirst("X-User-Id")).isEqualTo("user-1");
    }

    private String signToken(String userId, String role, Instant expiry) {
        return Jwts.builder()
                .subject(userId)
                .claim("role", role)
                .issuedAt(Date.from(Instant.now().minusSeconds(60)))
                .expiration(Date.from(expiry))
                .signWith(privateKey, Jwts.SIG.RS256)
                .compact();
    }

    private static String toPem(PublicKey key) {
        return "-----BEGIN PUBLIC KEY-----"
                + Base64.getEncoder().encodeToString(key.getEncoded())
                + "-----END PUBLIC KEY-----";
    }

    private static MockServerWebExchange exchangeFor(String path, String... headerPairs) {
        MockServerHttpRequest.BaseBuilder<?> builder = MockServerHttpRequest.get(path);
        for (int i = 0; i < headerPairs.length; i += 2) {
            builder.header(headerPairs[i], headerPairs[i + 1]);
        }
        return MockServerWebExchange.from(builder.build());
    }

    private FilterOutcome runFilter(MockServerWebExchange exchange) {
        AtomicReference<ServerWebExchange> forwarded = new AtomicReference<>();
        AtomicReference<Boolean> chained = new AtomicReference<>(false);
        GatewayFilterChain chain = ex -> {
            forwarded.set(ex);
            chained.set(true);
            return Mono.empty();
        };
        filter.filter(exchange, chain).block();
        return new FilterOutcome(exchange, forwarded.get(), chained.get());
    }

    private record FilterOutcome(MockServerWebExchange original, ServerWebExchange forwarded, boolean chained) {
    }
}
