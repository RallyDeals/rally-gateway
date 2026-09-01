package com.rally.gateway.composition;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.factory.rewrite.RewriteFunction;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import reactor.core.publisher.Mono;

/**
 * Response composition for the deal-returning endpoints of {@code DealController}
 * (rally-deal no longer enriches these with product details itself). These routes still
 * proxy normally to the Deal Service — same as a plain declarative route — so they run
 * through the gateway's usual filter chain (JWT auth, rate limiting, request id,
 * fallback error handling all still apply). A {@code modifyResponseBody} filter then
 * rewrites the already-proxied response body, fetching product details from the Catalog
 * Service (via {@link ProductEnrichmentClient}) and merging them in before the response
 * reaches the client.
 *
 * <p>{@code @Order(HIGHEST_PRECEDENCE)} makes sure these routes are matched before the
 * declarative catch-all {@code deal} route in application.yml (Path={@code /deals/**}),
 * which would otherwise win first for the same paths and return the raw, unenriched
 * response. {@code GET /deals/analytics} is deliberately left with no rewrite filter and
 * placed before {@code deal-get-composed}, whose {@code Path=/deals/{id}} would otherwise
 * match that literal path too.
 */
@Configuration
@RequiredArgsConstructor
public class DealCompositionRoutes {

    private static final String CONTEXT = "deal-service";

    private final ProductEnrichmentClient productEnrichmentClient;
    private final JsonRewriteSupport jsonRewriteSupport;

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public RouteLocator dealCompositionRouteLocator(RouteLocatorBuilder builder,
            @Value("${DEAL_SERVICE_URI:http://localhost:8085}") String dealServiceUri) {

        RewriteFunction<String, String> enrichPage = (exchange, body) -> enrichPage(body);
        RewriteFunction<String, String> enrichSingle = (exchange, body) -> enrichSingle(body);

        return builder.routes()
                .route("deal-list-composed", r -> r.method(HttpMethod.GET).and().path("/deals")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichPage))
                        .uri(dealServiceUri))
                .route("deal-create-composed", r -> r.method(HttpMethod.POST).and().path("/deals")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichSingle))
                        .uri(dealServiceUri))
                .route("deal-analytics", r -> r.method(HttpMethod.GET).and().path("/deals/analytics")
                        .uri(dealServiceUri))
                .route("deal-cancel-composed", r -> r.method(HttpMethod.POST).and().path("/deals/{id}/cancel")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichSingle))
                        .uri(dealServiceUri))
                .route("deal-update-composed", r -> r.method(HttpMethod.PATCH).and().path("/deals/{id}")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichSingle))
                        .uri(dealServiceUri))
                .route("deal-get-composed", r -> r.method(HttpMethod.GET).and().path("/deals/{id}")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichSingle))
                        .uri(dealServiceUri))
                .build();
    }

    private Mono<String> enrichPage(String body) {
        return jsonRewriteSupport.enrichArray(body, "content", "productId",
                productEnrichmentClient::fetchProducts, productEnrichmentClient::mergeProductFields, CONTEXT);
    }

    private Mono<String> enrichSingle(String body) {
        return jsonRewriteSupport.enrichSingle(body, "productId",
                productEnrichmentClient::fetchProducts, productEnrichmentClient::mergeProductFields, CONTEXT);
    }
}
