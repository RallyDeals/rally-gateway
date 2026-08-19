package com.rally.gateway.filter;

import com.rally.gateway.config.GatewayProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RequestIdGlobalFilterTest {

    private RequestIdGlobalFilter filter;

    @BeforeEach
    void setUp() {
        filter = new RequestIdGlobalFilter(new GatewayProperties());
    }

    @Test
    void injectsFreshXRequestId() {
        FilterOutcome outcome = runFilter(exchangeFor("/products"));

        assertThat(outcome.chained).isTrue();
        String requestId = outcome.forwarded.getRequest().getHeaders().getFirst("X-Request-Id");
        assertThat(requestId).isNotBlank();
        assertThat(UUID.fromString(requestId)).isNotNull();
    }

    @Test
    void stripsClientSuppliedXRequestId() {
        FilterOutcome outcome = runFilter(exchangeFor("/products", "X-Request-Id", "client-chosen-id"));

        String requestId = outcome.forwarded.getRequest().getHeaders().getFirst("X-Request-Id");
        assertThat(requestId).isNotEqualTo("client-chosen-id");
        assertThat(UUID.fromString(requestId)).isNotNull();
    }

    @Test
    void generatesUniqueIdsPerRequest() {
        String first = runFilter(exchangeFor("/products")).forwarded.getRequest().getHeaders().getFirst("X-Request-Id");
        String second = runFilter(exchangeFor("/products")).forwarded.getRequest().getHeaders().getFirst("X-Request-Id");

        assertThat(first).isNotEqualTo(second);
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
        return new FilterOutcome(forwarded.get(), chained.get());
    }

    private record FilterOutcome(ServerWebExchange forwarded, boolean chained) {
    }
}
