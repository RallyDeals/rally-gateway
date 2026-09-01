package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Configuration
@RequiredArgsConstructor
public class SellerCompositionRoute {
    private static final Logger log = LoggerFactory.getLogger(SellerCompositionRoute.class);
    private static final String CONTEXT = "auth-service (sellers)";

    private final WebClient catalogServiceWebClient;
    private final ObjectMapper objectMapper;
    private final JsonRewriteSupport jsonRewriteSupport;

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public RouteLocator sellerCompositionRouteLocator(RouteLocatorBuilder builder,
                                                           @Value("${AUTH_SERVICE_URI:http://localhost:8084}") String authServiceUri) {
        RewriteFunction<String, String> enrichPage = (exchange, body) -> enrichPage(body);
        RewriteFunction<String, String> enrichSingle = (exchange, body) -> enrichSingle(body);

        return builder.routes()
                .route("sellers-composition", r -> r.method(HttpMethod.GET)
                        .and().path("/users/sellers")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichPage))
                        .uri(authServiceUri))
                .route("seller-composition", r -> r.method(HttpMethod.GET)
                        .and().path("/users/sellers/{id}")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichSingle))
                        .uri(authServiceUri))
                .build();
    }

    private Mono<String> enrichPage(String body) {
        return jsonRewriteSupport.enrichArray(body, "items", "id",
                this::fetchSellerProductsInfo, this::mergeUserFields, CONTEXT);
    }

    private Mono<String> enrichSingle(String body) {
        return jsonRewriteSupport.enrichSingle(body, "id",
                this::fetchSellerProductsInfo, this::mergeUserFields, CONTEXT);
    }

    private Mono<Map<String, JsonNode>> fetchSellerProductsInfo(Set<String> sellerIds) {
        ArrayNode ids = objectMapper.createArrayNode();
        sellerIds.forEach(ids::add);

        return catalogServiceWebClient.post()
                .uri("/internal/products/sellers-summary")
                .bodyValue(ids)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(response -> indexUsersById(response.path("sellers")))
                .onErrorResume(ex -> {
                    log.warn("Seller summary batch lookup failed for {} ids: {}", sellerIds.size(), ex.toString());
                    return Mono.just(Map.of());
                });
    }

    private Map<String, JsonNode> indexUsersById(JsonNode users) {
        Map<String, JsonNode> usersById = new HashMap<>();
        for (JsonNode user : users) {
            usersById.put(user.path("sellerId").asText(), user);
        }
        return usersById;
    }

    private void mergeUserFields(ObjectNode sellerNode, JsonNode sellerInfo) {
        sellerNode.put("totalProducts", sellerInfo.path("totalProducts").asInt(0));
        sellerNode.put("totalPendingProducts", sellerInfo.path("totalPendingProducts").asInt(0));
    }
}
