package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
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

    private final ParticipationPageClient participationPageClient;
    private final DealBulkFetchClient dealBulkFetchClient;
    private final ObjectMapper objectMapper;

    public Mono<JsonNode> buildMyDealsSummaryFor(String userId) {
        return participationPageClient.fetchAllParticipations(userId, null)
                .flatMap(this::bulkFetchDealsFor)
                .map(this::buildMyDealsSummaryResponse);
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