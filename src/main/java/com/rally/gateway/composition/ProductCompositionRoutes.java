package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.factory.rewrite.RewriteFunction;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Set;

/**
 * Response composition for {@code GET /products} (list) and {@code GET /products/{id}}:
 * both proxy normally to the Catalog Service (same as a plain declarative route, so JWT
 * auth/rate limiting/etc. from the gateway's usual filter chain still apply), then a
 * {@code modifyResponseBody} filter calls the Deal Service ({@code GET
 * /deals?productId={id}}, once per distinct product for the list) and attaches the
 * matching deals to each product as a {@code deals} array.
 *
 * <p>{@code @Order(HIGHEST_PRECEDENCE)} makes sure these routes are matched before the
 * declarative catch-all {@code catalog} route in application.yml (Path=
 * {@code /products/**}). {@code GET /products/admin} is deliberately left with no rewrite
 * filter and placed before {@code product-get-composed}, whose {@code Path=/products/{id}}
 * would otherwise match that literal path too.
 */
@Configuration
@RequiredArgsConstructor
public class ProductCompositionRoutes {

    private static final Logger log = LoggerFactory.getLogger(ProductCompositionRoutes.class);
    private static final String CONTEXT = "catalog-service";

    private final WebClient dealServiceWebClient;
    private final ObjectMapper objectMapper;
    private final JsonRewriteSupport jsonRewriteSupport;

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public RouteLocator productCompositionRouteLocator(RouteLocatorBuilder builder,
            @Value("${CATALOG_SERVICE_URI:http://localhost:8083}") String catalogServiceUri) {

        RewriteFunction<String, String> enrichProduct = (exchange, body) -> enrichProduct(body);
        RewriteFunction<String, String> enrichProductList = (exchange, body) -> enrichProductList(body);

        return builder.routes()
                .route("product-list-composed", r -> r.method(HttpMethod.GET).and().path("/products")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichProductList))
                        .uri(catalogServiceUri))
                .route("product-admin-passthrough", r -> r.method(HttpMethod.GET).and().path("/products/admin")
                        .uri(catalogServiceUri))
                .route("product-get-composed", r -> r.method(HttpMethod.GET).and().path("/products/{id}")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichProduct))
                        .uri(catalogServiceUri))
                .build();
    }

    private Mono<String> enrichProductList(String body) {
        return jsonRewriteSupport.enrichArray(body, "items", "id", this::fetchDeals, this::mergeDeals, CONTEXT);
    }

    private Mono<String> enrichProduct(String body) {
        return jsonRewriteSupport.enrichSingle(body, "id", this::fetchDeals, this::mergeDeals, CONTEXT);
    }

    private void mergeDeals(ObjectNode productNode, JsonNode deals) {
        productNode.set("deals", deals);
    }

    /** Fetches each product's deals independently — one failing lookup doesn't affect the rest. */
    private Mono<Map<String, JsonNode>> fetchDeals(Set<String> productIds) {
        return Flux.fromIterable(productIds)
                .flatMap(id -> dealServiceWebClient.get()
                        .uri(uriBuilder -> uriBuilder.path("/deals").queryParam("productId", id).build())
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .map(dealsPage -> {
                            JsonNode content = dealsPage.path("content");
                            JsonNode deals = content.isArray() ? content : objectMapper.createArrayNode();
                            return new SimpleEntry<>(id, deals);
                        })
                        .onErrorResume(ex -> {
                            log.warn("Deal lookup failed for productId={}: {}", id, ex.toString());
                            return Mono.empty();
                        }))
                .collectMap(SimpleEntry::getKey, SimpleEntry::getValue);
    }
}
