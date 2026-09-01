package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Fetches product details from the Catalog Service in a single batched call and merges
 * them onto deal-like JSON nodes. Shared by {@link DealCompositionRoutes} and
 * {@link ProfileMyDealsService} so both stay on the same product-enrichment behavior.
 */
@Component
@RequiredArgsConstructor
public class ProductEnrichmentClient {

    private static final Logger log = LoggerFactory.getLogger(ProductEnrichmentClient.class);

    private final WebClient catalogServiceWebClient;
    private final ObjectMapper objectMapper;

    /** Extracts distinct {@code productId}s from each node, fetches them, and merges the results in place. */
    public Mono<Void> enrichDealsWithProducts(ArrayNode deals) {
        Set<String> productIds = new LinkedHashSet<>();
        for (JsonNode deal : deals) {
            String productId = deal.path("productId").asText(null);
            if (productId != null) {
                productIds.add(productId);
            }
        }
        if (productIds.isEmpty()) {
            return Mono.empty();
        }

        return fetchProducts(productIds).doOnNext(products -> {
            for (JsonNode deal : deals) {
                if (!(deal instanceof ObjectNode dealNode)) {
                    continue;
                }
                String productId = deal.path("productId").asText(null);
                JsonNode product = productId == null ? null : products.get(productId);
                if (product != null) {
                    mergeProductFields(dealNode, product);
                }
            }
        }).then();
    }

    public Mono<Map<String, JsonNode>>fetchProducts(Set<String> productIds) {
        ArrayNode ids = objectMapper.createArrayNode();
        productIds.forEach(ids::add);

        return catalogServiceWebClient.post()
                .uri("/internal/products/batch")
                .bodyValue(ids)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(this::indexProductsById)
                .onErrorResume(ex -> {
                    log.warn("Product batch lookup failed for {} ids: {}", productIds.size(), ex.toString());
                    return Mono.just(Map.of());
                });
    }

    private Map<String, JsonNode> indexProductsById(JsonNode products) {
        Map<String, JsonNode> byId = new HashMap<>();
        for (JsonNode product : products) {
            byId.put(product.path("id").asText(), product);
        }
        return byId;
    }

    public void mergeProductFields(ObjectNode dealNode, JsonNode product) {
        dealNode.put("productName", product.path("productName").asText(null));
        dealNode.put("productImageUrl", product.path("productImageUrl").asText(null));
        dealNode.put("category", product.path("category").path("name").asText(null));
        dealNode.put("sku", product.path("sku").asText(null));
        dealNode.put("sellerName", product.path("sellerName").asText(null));
    }
}
