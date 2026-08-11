package com.rally.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rally.security.JwtProperties;
import com.rally.security.JwtService;
import com.rally.gateway.config.GatewayProperties;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class JwtAuthGlobalFilterTest {

    private JwtService jwtService;
    private JwtAuthGlobalFilter filter;
    private String secret;

    @BeforeEach
    void setUp() {
        // This unit test does NOT need the production secret — the filter logic only
        // cares that tokens and validation share a key. Generate a random one at
        // runtime (64 bytes -> 88 chars, well above jjwt's 256-bit minimum), so no
        // secret string exists in source code.
        byte[] keyBytes = new byte[64];
        new SecureRandom().nextBytes(keyBytes);
        secret = Base64.getEncoder().encodeToString(keyBytes);

        JwtProperties jwtProperties = new JwtProperties();
        jwtProperties.setSecret(secret);
        jwtProperties.setIssuer("test");
        jwtService = new JwtService(jwtProperties);

        GatewayProperties gatewayProperties = new GatewayProperties();
        // Mirror Spring Boot's auto-configured ObjectMapper (ErrorResponse carries an Instant).
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        filter = new JwtAuthGlobalFilter(jwtService, gatewayProperties, new ErrorResponseWriter(objectMapper));
    }

    @Test
    void validTokenInjectsIdentityHeadersAndStripsToken() {
        String token = jwtService.generateAccessToken("user-1", List.of("SELLER", "BUYER"));

        FilterOutcome outcome = runFilter(exchangeFor("/products",
                "Authorization", "Bearer " + token,
                "X-User-Id", "spoofed-user",
                "X-User-Role", "ADMIN"));

        assertThat(outcome.chained).isTrue();
        HttpHeaders headers = outcome.forwarded.getRequest().getHeaders();
        assertThat(headers.getFirst("X-User-Id")).isEqualTo("user-1");
        assertThat(headers.getFirst("X-User-Role")).isEqualTo("SELLER,BUYER");
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
        SecretKey key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        String expired = Jwts.builder()
                .subject("user-1")
                .issuer("test")
                .issuedAt(new Date(System.currentTimeMillis() - 60_000))
                .expiration(new Date(System.currentTimeMillis() - 30_000))
                .signWith(key)
                .compact();

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
