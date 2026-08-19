package com.rally.gateway.config;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimitingConfigTest {

    private final KeyResolver resolver = new RateLimitingConfig().userKeyResolver();

    @Test
    void authenticatedRequestIsKeyedByUserId() {
        String key = resolve("/products", "X-User-Id", "user-1");
        assertThat(key).isEqualTo("user:user-1");
    }

    @Test
    void requestWithoutUserFallsBackToIp() {
        MockServerWebExchange exchange = exchangeFor("/auth/login", "remote", "203.0.113.7");
        String key = resolver.resolve(exchange).block();
        assertThat(key).isEqualTo("ip:203.0.113.7");
    }

    @Test
    void blankUserIdFallsBackToIp() {
        String key = resolve("/products", "X-User-Id", "  ");
        assertThat(key).startsWith("ip:");
    }

    @Test
    void usersAndIpsNeverCollide() {
        assertThat(resolve("/a", "X-User-Id", "203.0.113.7")).isEqualTo("user:203.0.113.7");
        String ipKey = resolver.resolve(exchangeFor("/a", "remote", "203.0.113.7")).block();
        assertThat(ipKey).isEqualTo("ip:203.0.113.7");
    }

    private String resolve(String path, String... headerPairs) {
        return resolver.resolve(exchangeFor(path, headerPairs)).block();
    }

    private static MockServerWebExchange exchangeFor(String path, String... headerPairs) {
        MockServerHttpRequest.BaseBuilder<?> builder = MockServerHttpRequest.get(path);
        for (int i = 0; i < headerPairs.length; i += 2) {
            if (headerPairs[i].equals("remote")) {
                builder.remoteAddress(new java.net.InetSocketAddress(headerPairs[i + 1], 4321));
            } else {
                builder.header(headerPairs[i], headerPairs[i + 1]);
            }
        }
        return MockServerWebExchange.from(builder.build());
    }
}
