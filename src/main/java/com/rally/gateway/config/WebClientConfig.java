package com.rally.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * WebClients used by the composition routes to make side-calls to the other service while
 * rewriting an already-proxied response ({@code DealCompositionRoutes} calls Catalog,
 * {@code ProductCompositionRoutes} calls Deal). Base URLs mirror the same env vars the
 * declarative routes use, so both stay in sync.
 *
 * <p>Every client carries {@link #propagationFilter(GatewayProperties)}: a single
 * {@code ExchangeFilterFunction} that stamps the gateway-owned {@code X-Correlation-Id}
 * and {@code traceparent} (published to the Reactor context by the gateway's trace
 * filter) onto each outgoing side-call. One definition covers all current and future
 * composition clients — no per-call-site header plumbing. Outside a request scope the
 * context is empty and calls pass through untouched.
 */
@Configuration
public class WebClientConfig {

    @Bean
    public ExchangeFilterFunction propagationFilter(GatewayProperties properties) {
        String correlationHeader = properties.getRequestIdHeader();
        String traceHeader = properties.getTraceparentHeader();
        return (request, next) -> Mono.deferContextual(context -> {
            ClientRequest.Builder stamped = ClientRequest.from(request);
            if (context.hasKey(correlationHeader)) {
                String correlationId = context.get(correlationHeader);
                if (correlationId != null && !correlationId.isBlank()) {
                    stamped.header(correlationHeader, correlationId);
                }
            }
            if (context.hasKey(traceHeader)) {
                String traceparent = context.get(traceHeader);
                if (traceparent != null && !traceparent.isBlank()) {
                    stamped.header(traceHeader, traceparent);
                }
            }
            return next.exchange(stamped.build());
        });
    }

    @Bean
    public WebClient catalogServiceWebClient(WebClient.Builder builder,
                                             ExchangeFilterFunction propagationFilter,
                                             @Value("${CATALOG_SERVICE_URI:http://localhost:8083}") String uri) {
        return builder.baseUrl(uri).filter(propagationFilter).build();
    }

    @Bean
    public WebClient dealServiceWebClient(WebClient.Builder builder,
                                          ExchangeFilterFunction propagationFilter,
                                          @Value("${DEAL_SERVICE_URI:http://localhost:8085}") String uri) {
        return builder.baseUrl(uri).filter(propagationFilter).build();
    }

    @Bean
    public WebClient authServiceWebClient(WebClient.Builder builder,
                                          ExchangeFilterFunction propagationFilter,
                                          @Value("${AUTH_SERVICE_URI:http://localhost:8084}") String uri) {
        return builder.baseUrl(uri).filter(propagationFilter).build();
    }

    @Bean
    public WebClient participationServiceWebClient(WebClient.Builder builder,
                                                   ExchangeFilterFunction propagationFilter,
                                                   @Value("${PARTICIPATION_SERVICE_URI:http://localhost:8086}") String uri) {
        return builder.baseUrl(uri).filter(propagationFilter).build();
    }

    @Bean
    public WebClient orderServiceWebClient(WebClient.Builder builder,
                                           ExchangeFilterFunction propagationFilter,
                                           @Value("${ORDER_SERVICE_URI:http://localhost:8081}") String uri) {
        return builder.baseUrl(uri).filter(propagationFilter).build();
    }
}
