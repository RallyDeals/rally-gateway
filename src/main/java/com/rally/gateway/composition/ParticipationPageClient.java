package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rally.gateway.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * Fetches one page of a user's participations from the Participation Service. Shared by
 * {@link ProfileMyDealsService} and {@link ProfileMyDealsSummaryService}, which both page
 * through the same {@code GET /participations} endpoint.
 */
@Component
@RequiredArgsConstructor
public class ParticipationPageClient {

    private static final Logger log = LoggerFactory.getLogger(ParticipationPageClient.class);

    private final WebClient participationServiceWebClient;
    private final ObjectMapper objectMapper;
    private final GatewayProperties gatewayProperties;

    public Mono<JsonNode> fetchParticipationsPage(String userId, int page, int size) {
        return participationServiceWebClient.get()
                .uri(uriBuilder -> uriBuilder.path("/participations")
                        .queryParam("userId", userId)
                        .queryParam("page", page)
                        .queryParam("size", size)
                        .build())
                .header(gatewayProperties.getUserIdHeader(), userId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnNext(node -> log.info(
                        "participations fetched: userId={} page={} size={} itemsReturned={} totalElements={} raw={}",
                        userId, page, size, node.path("participations").size(),
                        node.path("totalElements").asInt(0), node))
                .onErrorResume(ex -> {
                    log.warn("participations page {} lookup failed for userId={}: {}", page, userId, ex.toString());
                    return Mono.just(objectMapper.createObjectNode());
                });
    }
}
