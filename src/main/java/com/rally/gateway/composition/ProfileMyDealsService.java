package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Builds the composed response for {@code GET /profile/my-deals}: the user's joined
 * deals, enriched with product details, filtered by participation/deal status.
 */
@Component
@RequiredArgsConstructor
public class ProfileMyDealsService {

    private static final Logger log = LoggerFactory.getLogger(ProfileMyDealsService.class);

    private final ParticipationPageClient participationPageClient;
    private final DealBulkFetchClient dealBulkFetchClient;
    private final ProductEnrichmentClient productEnrichmentClient;
    private final ObjectMapper objectMapper;

    public int parseIntParam(ServerWebExchange exchange, String paramName, int defaultValue) {
        String raw = exchange.getRequest().getQueryParams().getFirst(paramName);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    public Set<String> parseStatusFilter(ServerWebExchange exchange, String paramName) {
        Set<String> result = new LinkedHashSet<>();
        for (String raw : exchange.getRequest().getQueryParams().getOrDefault(paramName, List.of())) {
            for (String part : raw.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    result.add(trimmed.toUpperCase(Locale.ROOT));
                }
            }
        }
        return result;
    }

    public String parseSingleStatusFilter(ServerWebExchange exchange, String paramName) {
        String raw = exchange.getRequest().getQueryParams().getFirst(paramName);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return raw.trim().toUpperCase(Locale.ROOT);
    }

    public Mono<JsonNode> buildMyDealsResponseFor(String userId, int page, int size,
            String participationStatusFilter, Set<String> dealStatusFilter) {
        return buildFilteredAllParticipantsResponse(userId, page, size, participationStatusFilter, dealStatusFilter);
    }

    private Mono<JsonNode> buildFilteredAllParticipantsResponse(String userId, int page, int size,
            String participationStatusFilter, Set<String> dealStatusFilter) {
        return participationPageClient.fetchAllParticipations(userId, participationStatusFilter)
                .flatMap(this::bulkFetchDealsFor)
                .flatMap(this::enrichWithProducts)
                .map(deals -> buildFilteredPage(deals, page, size, dealStatusFilter));
    }
    private Mono<JsonNode> bulkFetchDealsFor(List<JsonNode> participations) {
        Set<String> dealIds = new LinkedHashSet<>();
        for (JsonNode p : participations) {
            String dealId = p.path("dealId").asText(null);
            if (dealId != null) {
                dealIds.add(dealId);
            }
        }
        log.info("joinedDealsBulk: extracted {} distinct dealIds from {} participation(s): {}",
                dealIds.size(), participations.size(), dealIds);
        return dealBulkFetchClient.bulkFetchDeals(dealIds);
    }

    private Mono<JsonNode> enrichWithProducts(JsonNode deals) {
        if (!deals.isArray() || deals.isEmpty()) {
            return Mono.just(deals);
        }
        return productEnrichmentClient.enrichDealsWithProducts((ArrayNode) deals).thenReturn(deals);
    }

    private JsonNode buildFilteredPage(JsonNode deals, int page, int size, Set<String> dealStatusFilter) {
        ArrayNode filtered = objectMapper.createArrayNode();
        if (deals.isArray()) {
            for (JsonNode deal : deals) {
                if (dealStatusFilter.isEmpty() || dealStatusFilter.contains(deal.path("status").asText(null))) {
                    filtered.add(deal);
                }
            }
        }

        int safePage = Math.max(page, 1);
        int from = Math.min((safePage - 1) * size, filtered.size());
        int to = Math.min(from + size, filtered.size());
        ArrayNode pageItems = objectMapper.createArrayNode();
        for (int i = from; i < to; i++) {
            JsonNode item = filtered.get(i);
            if (item != null) {
                pageItems.add(item);
            }
        }

        log.info("buildFilteredPage: dealsFetched={} dealStatusFilter={} dealsAfterFilter={} page={} size={} itemsReturned={}",
                deals.isArray() ? deals.size() : 0, dealStatusFilter, filtered.size(),
                safePage, size, pageItems.size());

        ObjectNode response = objectMapper.createObjectNode();
        response.set("deals", pageItems);
        response.put("page", safePage);
        response.put("size", size);
        response.put("totalElements", filtered.size());
        return response;
    }
}