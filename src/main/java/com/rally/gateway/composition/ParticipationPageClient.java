package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rally.gateway.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

/**
 * Fetches participations from the Participation Service. Shared by
 * {@link ProfileMyDealsService} and {@link ProfileMyDealsSummaryService}, which both page
 * through the same {@code GET /participations} endpoint, optionally filtered by status.
 */
@Component
@RequiredArgsConstructor
public class ParticipationPageClient {

    private static final Logger log = LoggerFactory.getLogger(ParticipationPageClient.class);

    private static final int ALL_PAGES_PAGE_SIZE = 100;
    private static final int ALL_PAGES_CONCURRENCY = 4;

    private final WebClient participationServiceWebClient;
    private final ObjectMapper objectMapper;
    private final GatewayProperties gatewayProperties;

    public Mono<JsonNode> fetchParticipationsPage(String userId, int page, int size, String status) {
        return participationServiceWebClient.get()
                .uri(uriBuilder -> {
                    uriBuilder.path("/participations")
                            .queryParam("userId", userId)
                            .queryParam("page", page)
                            .queryParam("size", size);
                    if (status != null && !status.isBlank()) {
                        uriBuilder.queryParam("status", status);
                    }
                    return uriBuilder.build();
                })
                .header(gatewayProperties.getUserIdHeader(), userId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnNext(node -> log.info(
                        "participations fetched: userId={} page={} size={} status={} itemsReturned={} totalElements={} raw={}",
                        userId, page, size, status, node.path("participations").size(),
                        node.path("totalElements").asInt(0), node))
                .onErrorResume(ex -> {
                    log.warn("participations page {} lookup failed for userId={}: {}", page, userId, ex.toString());
                    return Mono.just(objectMapper.createObjectNode());
                });
    }

    /**
     * Fetches every page of the user's participations (concurrency capped). The participation
     * endpoint is 1-based, so page 1 is fetched first and the remaining pages follow from it.
     */
    public Mono<List<JsonNode>> fetchAllParticipations(String userId, String status) {
        return fetchParticipationsPage(userId, 1, ALL_PAGES_PAGE_SIZE, status)
                .flatMap(firstPage -> {
                    List<JsonNode> items = new ArrayList<>(toList(firstPage.path("participations")));
                    int totalElements = firstPage.path("totalElements").asInt(items.size());
                    int totalPages = totalElements == 0
                            ? 0
                            : (totalElements + ALL_PAGES_PAGE_SIZE - 1) / ALL_PAGES_PAGE_SIZE;

                    log.debug("participation fan-out for userId={}: totalElements={} totalPages={} status={}",
                            userId, totalElements, totalPages, status);

                    if (totalPages <= 1) {
                        return Mono.just(items);
                    }
                    return Flux.range(2, totalPages - 1)
                            .flatMap(page -> fetchParticipationsPage(userId, page, ALL_PAGES_PAGE_SIZE, status),
                                    ALL_PAGES_CONCURRENCY)
                            .collectList()
                            .map(morePages -> {
                                for (JsonNode page : morePages) {
                                    items.addAll(toList(page.path("participations")));
                                }
                                log.debug("participation fan-out complete for userId={}: fetched {} participations across {} pages",
                                        userId, items.size(), totalPages);
                                return items;
                            });
                });
    }

    private List<JsonNode> toList(JsonNode arrayNode) {
        List<JsonNode> list = new ArrayList<>();
        if (arrayNode.isArray()) {
            arrayNode.forEach(list::add);
        }
        return list;
    }
}