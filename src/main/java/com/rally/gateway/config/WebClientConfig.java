package com.rally.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * WebClients used by the composition routes to make side-calls to the other service while
 * rewriting an already-proxied response ({@code DealCompositionRoutes} calls Catalog,
 * {@code ProductCompositionRoutes} calls Deal). Base URLs mirror the same env vars the
 * declarative routes use, so both stay in sync.
 */
@Configuration
public class WebClientConfig {

    @Bean
    public WebClient catalogServiceWebClient(WebClient.Builder builder,
            @Value("${CATALOG_SERVICE_URI:http://localhost:8083}") String uri) {
        return builder.baseUrl(uri).build();
    }

    @Bean
    public WebClient dealServiceWebClient(WebClient.Builder builder,
            @Value("${DEAL_SERVICE_URI:http://localhost:8081}") String uri) {
        return builder.baseUrl(uri).build();
    }
}
