package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Set;

/**
 * Batches deal lookups by id against the Deal Service. Shared by
 * {@link ProfileMyDealsService} and {@link ProfileMyDealsSummaryService}.
 */
@Component
@RequiredArgsConstructor
public class DealBulkFetchClient {

    private static final Logger log = LoggerFactory.getLogger(DealBulkFetchClient.class);

    private final WebClient dealServiceWebClient;
    private final ObjectMapper objectMapper;

    public Mono<JsonNode> bulkFetchDeals(Set<String> dealIds) {
        if (dealIds.isEmpty()) {
            log.info("bulkFetchDeals: no dealIds to fetch, skipping deals/bulk call");
            return Mono.just(objectMapper.createArrayNode());
        }

        ObjectNode requestBody = objectMapper.createObjectNode();
        ArrayNode idsNode = requestBody.putArray("ids");
        dealIds.forEach(idsNode::add);

        return dealServiceWebClient.post()
                .uri("/deals/bulk")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnNext(node -> log.info("deals/bulk returned {} deals for {} requested ids, raw={}",
                        node.isArray() ? node.size() : 0, dealIds.size(), node))
                .onErrorResume(ex -> {
                    log.warn("deals/bulk lookup failed for {} ids: {}", dealIds.size(), ex.toString());
                    return Mono.just(objectMapper.createArrayNode());
                });
    }
}
