package com.rally.gateway.filter;

import com.rally.gateway.config.GatewayProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.Ordered;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class TracePropagationGlobalFilterTest {

    private static final Pattern TRACEPARENT =
            Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]");
    private static final String CLIENT_TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private TracePropagationGlobalFilter filter;

    @BeforeEach
    void setUp() {
        filter = new TracePropagationGlobalFilter(new GatewayProperties());
    }

    @Test
    void replacesClientTraceparentAndStripsTracestate() {
        // Zero-trust: client-supplied trace context is never forwarded, even when valid.
        FilterOutcome outcome = runFilter(
                exchangeFor("/products", "traceparent", CLIENT_TRACEPARENT, "tracestate", "rojo=1"));

        assertThat(outcome.chained).isTrue();
        String traceparent = outcome.forwarded.getRequest().getHeaders().getFirst("traceparent");
        assertThat(traceparent).isNotEqualTo(CLIENT_TRACEPARENT);
        assertThat(traceparent).matches(TRACEPARENT);
        assertThat(outcome.forwarded.getRequest().getHeaders().getFirst("tracestate")).isNull();
    }

    @Test
    void generatesValidTraceparentWhenAbsent() {
        FilterOutcome outcome = runFilter(exchangeFor("/products"));

        String traceparent = outcome.forwarded.getRequest().getHeaders().getFirst("traceparent");
        assertThat(traceparent).matches(TRACEPARENT);
    }

    @Test
    void replacesInvalidTraceparentAndDropsStaleTracestate() {
        FilterOutcome outcome = runFilter(
                exchangeFor("/products", "traceparent", "garbage", "tracestate", "rojo=1"));

        String traceparent = outcome.forwarded.getRequest().getHeaders().getFirst("traceparent");
        assertThat(traceparent).isNotEqualTo("garbage");
        assertThat(traceparent).matches(TRACEPARENT);
        assertThat(outcome.forwarded.getRequest().getHeaders().getFirst("tracestate")).isNull();
    }

    @Test
    void generatesUniqueTraceIdsPerRequest() {
        String first = runFilter(exchangeFor("/products"))
                .forwarded.getRequest().getHeaders().getFirst("traceparent");
        String second = runFilter(exchangeFor("/products"))
                .forwarded.getRequest().getHeaders().getFirst("traceparent");

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void publishesCorrelationIdAsBaggage() {
        FilterOutcome outcome = runFilter(exchangeFor("/products",
                "X-Correlation-Id", "3d3483d4-f4ba-41c9-ada2-993f71827083"));

        String baggage = outcome.forwarded.getRequest().getHeaders().getFirst("baggage");
        assertThat(baggage).contains("X-Correlation-Id=3d3483d4-f4ba-41c9-ada2-993f71827083");
    }

    @Test
    void appendsToExistingBaggageInsteadOfOverwriting() {
        FilterOutcome outcome = runFilter(exchangeFor("/products",
                "X-Correlation-Id", "cid-1", "baggage", "userId=u-9"));

        String baggage = outcome.forwarded.getRequest().getHeaders().getFirst("baggage");
        assertThat(baggage).contains("userId=u-9");
        assertThat(baggage).contains("X-Correlation-Id=cid-1");
    }

    @Test
    void runsRightAfterRequestIdFilter() {
        assertThat(filter.getOrder()).isEqualTo(Ordered.HIGHEST_PRECEDENCE + 1);
    }

    @Test
    void echoesTraceparentInResponse() {
        FilterOutcome outcome = runFilter(exchangeFor("/products"));
        outcome.forwarded.getResponse().setComplete().block();

        String traceparent = outcome.forwarded.getRequest().getHeaders().getFirst("traceparent");
        assertThat(traceparent).matches(TRACEPARENT);
        assertThat(outcome.forwarded.getResponse().getHeaders().getFirst("traceparent"))
                .isEqualTo(traceparent);
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
