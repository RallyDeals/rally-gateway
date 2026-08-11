package com.rally.gateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Catches failures while forwarding to a downstream service (connection refused on an
 * unrouted/unbuilt service, response timeout, backend errors) and answers with a clean
 * {@code 503 Service Unavailable} in the same {@code ErrorResponse} shape the rest of
 * the platform uses — instead of a raw gateway 500/502 page.
 *
 * <p>Runs after {@link JwtAuthGlobalFilter} so authentication/authorization still apply
 * first; a request without a valid token gets its 401 before this filter is reached.
 * Once the response is committed (e.g. a partial body already sent), the original error
 * is re-thrown so nothing is double-written.
 */
@Component
public class FallbackGlobalFilter implements GlobalFilter, Ordered {

    private final ErrorResponseWriter errorResponseWriter;

    public FallbackGlobalFilter(ErrorResponseWriter errorResponseWriter) {
        this.errorResponseWriter = errorResponseWriter;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return chain.filter(exchange).onErrorResume(error -> {
            if (exchange.getResponse().isCommitted()) {
                return Mono.error(error);
            }
            return errorResponseWriter.write(
                    exchange,
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Service Unavailable",
                    "The requested service is currently unavailable. Please try again later.");
        });
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }
}
