package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Response composition for the deal-returning endpoints of {@code DealController}
 * (rally-deal no longer enriches these with product details itself). These routes still
 * proxy normally to the Deal Service — same as a plain declarative route — so they run
 * through the gateway's usual filter chain (JWT auth, rate limiting, request id,
 * fallback error handling all still apply). A {@code modifyResponseBody} filter then
 * rewrites the already-proxied response body, fetching product details from the Catalog
 * Service and merging them in before the response reaches the client.
 *
 * <p>{@code @Order(HIGHEST_PRECEDENCE)} makes sure these routes are matched before the
 * declarative catch-all {@code deal} route in application.yml (Path={@code /deals/**}),
 * which would otherwise win first for the same paths and return the raw, unenriched
 * response. {@code GET /deals/analytics} is deliberately left with no rewrite filter and
 * placed before {@code deal-get-composed}, whose {@code Path=/deals/{id}} would otherwise
 * match that literal path too.
 */
@Configuration
public class DealCompositionRoutes {

    private static final Logger log = LoggerFactory.getLogger(DealCompositionRoutes.class);

    private final WebClient catalogServiceWebClient;
    private final ObjectMapper objectMapper;

    public DealCompositionRoutes(WebClient catalogServiceWebClient, ObjectMapper objectMapper) {
        this.catalogServiceWebClient = catalogServiceWebClient;
        this.objectMapper = objectMapper;
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public RouteLocator dealCompositionRouteLocator(RouteLocatorBuilder builder,
            @Value("${DEAL_SERVICE_URI:http://localhost:8081}") String dealServiceUri) {

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
        JsonNode root = readTree(body);
        if (root == null) {
            return Mono.just(body);
        }
        JsonNode contentNode = root.path("content");
        if (!contentNode.isArray() || contentNode.isEmpty()) {
            return Mono.just(body);
        }
        ArrayNode content = (ArrayNode) contentNode;

        Set<String> productIds = new LinkedHashSet<>();
        for (JsonNode deal : content) {
            String productId = deal.path("productId").asText(null);
            if (productId != null) {
                productIds.add(productId);
            }
        }
        if (productIds.isEmpty()) {
            return Mono.just(body);
        }

        return fetchProducts(productIds).map(products -> {
            for (JsonNode deal : content) {
                if (!(deal instanceof ObjectNode dealNode)) {
                    continue;
                }
                String productId = deal.path("productId").asText(null);
                JsonNode product = productId == null ? null : products.get(productId);
                if (product != null) {
                    mergeProductFields(dealNode, product);
                }
            }
            return writeValueAsString(root, body);
        });
    }

    private Mono<String> enrichSingle(String body) {
        JsonNode root = readTree(body);
        if (!(root instanceof ObjectNode dealNode)) {
            return Mono.just(body);
        }
        String productId = root.path("productId").asText(null);
        if (productId == null) {
            return Mono.just(body);
        }
        return catalogServiceWebClient.get()
                .uri("/internal/products/{id}", productId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnNext(product -> mergeProductFields(dealNode, product))
                .onErrorResume(ex -> {
                    log.warn("Product lookup failed for productId={}: {}", productId, ex.toString());
                    return Mono.empty();
                })
                .then(Mono.fromSupplier(() -> writeValueAsString(root, body)));
    }

    private Mono<Map<String, JsonNode>> fetchProducts(Set<String> productIds) {
        return Flux.fromIterable(productIds)
                .flatMap(id -> catalogServiceWebClient.get()
                        .uri("/internal/products/{id}", id)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .map(node -> new SimpleEntry<>(id, node))
                        .onErrorResume(ex -> {
                            log.warn("Product lookup failed for productId={}: {}", id, ex.toString());
                            return Mono.empty();
                        }))
                .collectMap(SimpleEntry::getKey, SimpleEntry::getValue);
    }

    private void mergeProductFields(ObjectNode dealNode, JsonNode product) {
        dealNode.put("productName", product.path("productName").asText(null));
        dealNode.put("productImageUrl", product.path("productImageUrl").asText(null));
        dealNode.put("category", product.path("category").path("name").asText(null));
        dealNode.put("sku", product.path("sku").asText(null));
        dealNode.put("sellerName", product.path("sellerName").asText(null));
    }

    private JsonNode readTree(String body) {
        try {
            return objectMapper.readTree(body);
        } catch (Exception ex) {
            log.warn("Failed to parse deal-service response body as JSON: {}", ex.toString());
            return null;
        }
    }

    private String writeValueAsString(JsonNode node, String fallback) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (Exception ex) {
            log.warn("Failed to serialize composed deal response, returning unenriched body: {}", ex.toString());
            return fallback;
        }
    }
}
