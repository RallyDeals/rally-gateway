package com.rally.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FallbackGlobalFilterTest {

    private final FallbackGlobalFilter filter =
            new FallbackGlobalFilter(new ErrorResponseWriter(new ObjectMapper().findAndRegisterModules()));

    @Test
    void downstreamFailureReturns503InErrorResponseShape() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/products"));
        GatewayFilterChain failingChain = ex -> Mono.error(new RuntimeException("connection refused"));

        filter.filter(exchange, failingChain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        String body = exchange.getResponse().getBodyAsString().block();
        assertThat(body).contains("\"status\":503")
                .contains("\"title\":\"Service Unavailable\"")
                .contains("\"message\":\"The requested service is currently unavailable. Please try again later.\"")
                .contains("\"path\":\"/products\"");
    }

    @Test
    void committedResponseReThrowsOriginalErrorWithoutWriting() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/products"));
        exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(new byte[]{1}))).block();
        GatewayFilterChain failingChain = ex -> Mono.error(new IllegalStateException("boom"));

        assertThatThrownBy(() -> filter.filter(exchange, failingChain).block())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
    }
}
