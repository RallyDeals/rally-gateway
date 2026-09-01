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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
        Mono<JsonNode> participationsMono = participationPageClient.fetchParticipationsPage(userId, page, size);

        return participationsMono.flatMap(root -> joinedDealsBulk(root)
                .flatMap(this::enrichWithProducts)
                .map(deals -> buildMyDealsResponse(deals, root, participationStatusFilter, dealStatusFilter)));
    }

    private Mono<JsonNode> joinedDealsBulk(JsonNode participationsRoot) {
        JsonNode participations = participationsRoot.path("participations");
        Set<String> dealIds = new LinkedHashSet<>();
        if (participations.isArray()) {
            for (JsonNode p : participations) {
                String dealId = p.path("dealId").asText(null);
                if (dealId != null) {
                    dealIds.add(dealId);
                }
            }
        }
        log.info("joinedDealsBulk: extracted {} distinct dealIds from {} participation(s): {}",
                dealIds.size(), participations.isArray() ? participations.size() : 0, dealIds);
        return dealBulkFetchClient.bulkFetchDeals(dealIds);
    }

    private Mono<JsonNode> enrichWithProducts(JsonNode deals) {
        if (!deals.isArray() || deals.isEmpty()) {
            return Mono.just(deals);
        }
        return productEnrichmentClient.enrichDealsWithProducts((ArrayNode) deals).thenReturn(deals);
    }

    private JsonNode buildMyDealsResponse(JsonNode deals, JsonNode participationsRoot,
            String participationStatusFilter, Set<String> dealStatusFilter) {
        Map<String, String> participationStatusByDealId = new LinkedHashMap<>();
        for (JsonNode p : participationsRoot.path("participations")) {
            String dealId = p.path("dealId").asText(null);
            if (dealId != null) {
                participationStatusByDealId.put(dealId, p.path("status").asText(null));
            }
        }

        ArrayNode filteredDeals = objectMapper.createArrayNode();

        if (deals.isArray()) {
            for (JsonNode deal : deals) {
                String dealStatus = deal.path("status").asText(null);
                String participationStatus = participationStatusByDealId.get(deal.path("id").asText(null));

                boolean matchesParticipation = participationStatusFilter == null
                        || participationStatusFilter.equals(participationStatus);
                boolean matchesDeal = dealStatusFilter.isEmpty()
                        || dealStatusFilter.contains(dealStatus);
                if (matchesParticipation && matchesDeal) {
                    ObjectNode item = deal.deepCopy();
                    item.put("participationStatus", participationStatus);
                    filteredDeals.add(item);
                }
            }
        }

        log.info("buildMyDealsResponse: dealsFetched={} participationStatusFilter={} dealStatusFilter={} "
                        + "dealsAfterFilter={}",
                deals.isArray() ? deals.size() : 0, participationStatusFilter, dealStatusFilter,
                filteredDeals.size());

        ObjectNode response = objectMapper.createObjectNode();
        response.set("deals", filteredDeals);
        response.put("page", participationsRoot.path("page").asInt(0));
        response.put("size", participationsRoot.path("size").asInt(0));
        response.put("totalElements", participationsRoot.path("totalElements").asInt(0));
        return response;
    }
}
