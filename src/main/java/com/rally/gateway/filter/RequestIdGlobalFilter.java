package com.rally.gateway.filter;

import com.rally.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Adds a {@code X-Request-Id} correlation header to every request for tracing and
 * idempotency bookkeeping. The client-supplied value is always removed first and replaced
 * with a fresh UUID — consistent with the gateway's trust model (services must only ever
 * see header values the gateway itself generated).
 */
@Component
public class RequestIdGlobalFilter implements GlobalFilter, Ordered {

    private final GatewayProperties properties;

    public RequestIdGlobalFilter(GatewayProperties properties) {
        this.properties = properties;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String requestIdHeader = properties.getRequestIdHeader();
        ServerWebExchange traced = exchange.mutate()
                .request(request -> request.headers(headers -> {
                    headers.remove(requestIdHeader);
                    headers.set(requestIdHeader, UUID.randomUUID().toString());
                }))
                .build();
        return chain.filter(traced);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
