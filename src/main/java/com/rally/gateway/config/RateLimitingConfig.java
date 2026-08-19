package com.rally.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Rate limiting key strategy (same pattern Mazadak's gateway uses with Spring Cloud
 * Gateway's {@code RequestRateLimiter} + Redis).
 *
 * <p>Each request is bucketed by the authenticated user's id — read from the
 * {@code X-User-Id} header the {@code JwtAuthGlobalFilter} injects after JWT validation.
 * Unauthenticated requests (public paths like {@code /auth/login}) have no user id, so
 * they fall back to the caller's IP address. Keys are prefixed so a user and an IP can
 * never collide in Redis (e.g. user "1.2.3.4" vs ip "1.2.3.4").
 */
@Configuration
public class RateLimitingConfig {

    @Bean
    public KeyResolver userKeyResolver() {
        return exchange -> {
            String userId = exchange.getRequest().getHeaders().getFirst("X-User-Id");

            if (userId == null || userId.isBlank()) {
                String ip = exchange.getRequest().getRemoteAddress() != null
                        ? exchange.getRequest().getRemoteAddress().getAddress().getHostAddress()
                        : "unknown";
                return Mono.just("ip:" + ip);
            }
            return Mono.just("user:" + userId);
        };
    }
}
