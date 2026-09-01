package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rally.gateway.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * Builds the composed response for {@code GET /profile/personal-info}: profile info from
 * Auth Service, joined-deals count from Participation Service, and orders count from Order
 * Service, fetched in parallel.
 */
@Component
@RequiredArgsConstructor
public class ProfilePersonalInfoService {

    private static final Logger log = LoggerFactory.getLogger(ProfilePersonalInfoService.class);

    private final WebClient authServiceWebClient;
    private final WebClient participationServiceWebClient;
    private final WebClient orderServiceWebClient;
    private final ObjectMapper objectMapper;
    private final GatewayProperties gatewayProperties;

    public Mono<JsonNode> buildProfileResponseFor(String userId) {
        Mono<JsonNode> profileInfoMono = authServiceWebClient.get()
                .uri("/auth/me")
                .header(gatewayProperties.getUserIdHeader(), userId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(ex -> {
                    log.warn("auth/me lookup failed for userId={}: {}", userId, ex.toString());
                    return Mono.just(objectMapper.createObjectNode());
                });
        Mono<JsonNode> participationsMono = participationServiceWebClient.get()
                .uri(uriBuilder -> uriBuilder.path("/participations")
                        .queryParam("userId", userId)
                        .build())
                .header(gatewayProperties.getUserIdHeader(), userId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(ex -> {
                    log.warn("participations lookup failed for userId={}: {}", userId, ex.toString());
                    return Mono.just(objectMapper.createObjectNode());
                });
        Mono<Long> joinedCountMono = participationsMono.map(this::joinedCount);

        Mono<Long> orderCountMono = orderServiceWebClient.get()
                .uri(uriBuilder -> uriBuilder.path("/api/orders/my")
                        .queryParam("page", 1)
                        .queryParam("limit", 1)
                        .build())
                .header(gatewayProperties.getUserIdHeader(), userId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(node -> node.path("total").asLong(0))
                .onErrorResume(ex -> {
                    log.warn("orders/my lookup failed for userId={}: {}", userId, ex.toString());
                    return Mono.just(0L);
                });
        return Mono.zip(profileInfoMono, joinedCountMono, orderCountMono)
                .map(tuple -> buildProfileResponse(tuple.getT1(), tuple.getT2(), tuple.getT3()));
    }

    private long joinedCount(JsonNode participationsRoot) {
        JsonNode participations = participationsRoot.path("participations");
        if (!participations.isArray()) {
            return 0;
        }
        long count = 0;
        for (JsonNode p : participations) {
            String status = p.path("status").asText(null);
            if ("ACTIVE".equals(status) || "LEFT".equals(status)) {
                count++;
            }
        }
        return count;
    }

    private JsonNode buildProfileResponse(JsonNode profileInfo, long joinedCount, long orderCount) {
        ObjectNode response = objectMapper.createObjectNode();
        response.set("info", profileInfo);
        response.put("dealsJoinedCount", joinedCount);
        response.put("ordersCount", orderCount);
        log.info("Profile response built for user {}", profileInfo.path("id").asText());
        return response;
    }
}
