package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds the composed response for {@code GET /profile/my-deals/summary}: fans out across
 * all pages of the user's participations (concurrency capped), bulk-fetches the deals, and
 * aggregates active-deal count and total savings.
 */
@Component
@RequiredArgsConstructor
public class ProfileMyDealsSummaryService {

    private static final Logger log = LoggerFactory.getLogger(ProfileMyDealsSummaryService.class);
    private static final int SUMMARY_PAGE_SIZE = 100;
    private static final int SUMMARY_PAGE_CONCURRENCY = 4;

    private final ParticipationPageClient participationPageClient;
    private final DealBulkFetchClient dealBulkFetchClient;
    private final ObjectMapper objectMapper;

    public Mono<JsonNode> buildMyDealsSummaryFor(String userId) {
        return fetchAllParticipations(userId)
                .flatMap(this::bulkFetchDealsFor)
                .map(this::buildMyDealsSummaryResponse);
    }

    private Mono<List<JsonNode>> fetchAllParticipations(String userId) {
        return participationPageClient.fetchParticipationsPage(userId, 1, SUMMARY_PAGE_SIZE)
                .flatMap(firstPage -> {
                    List<JsonNode> items = new ArrayList<>(toList(firstPage.path("participations")));
                    int totalElements = firstPage.path("totalElements").asInt(items.size());
                    int totalPages = totalElements == 0 ? 0
                            : (totalElements + SUMMARY_PAGE_SIZE - 1) / SUMMARY_PAGE_SIZE;

                    log.debug("summary fan-out for userId={}: totalElements={} totalPages={}",
                            userId, totalElements, totalPages);

                    if (totalPages <= 1) {
                        return Mono.just(items);
                    }
                    // /participations is 1-based; page 1 is already fetched above, so the
                    // remaining pages are 2..totalPages.
                    return Flux.range(2, totalPages - 1)
                            .flatMap(page -> participationPageClient.fetchParticipationsPage(userId, page, SUMMARY_PAGE_SIZE),
                                    SUMMARY_PAGE_CONCURRENCY)
                            .collectList()
                            .map(morePages -> {
                                for (JsonNode page : morePages) {
                                    items.addAll(toList(page.path("participations")));
                                }
                                log.debug("summary fan-out complete for userId={}: fetched {} participations across {} pages",
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

    private Mono<JsonNode> bulkFetchDealsFor(List<JsonNode> participations) {
        Set<String> dealIds = new LinkedHashSet<>();
        for (JsonNode p : participations) {
            String dealId = p.path("dealId").asText(null);
            if (dealId != null) {
                dealIds.add(dealId);
            }
        }
        return dealBulkFetchClient.bulkFetchDeals(dealIds);
    }

    private JsonNode buildMyDealsSummaryResponse(JsonNode deals) {
        long activeCount = 0;
        BigDecimal savedAmount = BigDecimal.ZERO;
        if (deals.isArray()) {
            for (JsonNode deal : deals) {
                String dealStatus = deal.path("status").asText(null);
                if ("ACTIVE".equals(dealStatus)) {
                    activeCount++;
                }
                if ("SUCCEEDED".equals(dealStatus)) {
                    BigDecimal originalPrice = deal.path("originalPrice").decimalValue();
                    BigDecimal dealPrice = deal.path("dealPrice").decimalValue();
                    savedAmount = savedAmount.add(originalPrice.subtract(dealPrice));
                }
            }
        }

        ObjectNode response = objectMapper.createObjectNode();
        response.put("activeDealsCount", activeCount);
        response.put("savedAmount", savedAmount);
        return response;
    }
}
