package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Shared body-rewrite plumbing for the {@code modifyResponseBody} composition routes:
 * parsing/serializing the proxied JSON body, and the common "collect ids from the body,
 * batch-fetch the related entities, merge matching fields back in" shape used by
 * {@link DealCompositionRoutes}, {@link ProductCompositionRoutes},
 * {@link ParticipantCompositionRoute}, and {@link SellerCompositionRoute}.
 */
@Component
@RequiredArgsConstructor
public class JsonRewriteSupport {

    private static final Logger log = LoggerFactory.getLogger(JsonRewriteSupport.class);

    private final ObjectMapper objectMapper;

    public JsonNode readTree(String body, String context) {
        try {
            return objectMapper.readTree(body);
        } catch (Exception ex) {
            log.warn("Failed to parse {} response body as JSON: {}", context, ex.toString());
            return null;
        }
    }

    public String writeValueAsString(JsonNode node, String fallback, String context) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (Exception ex) {
            log.warn("Failed to serialize composed {} response, returning unenriched body: {}", context, ex.toString());
            return fallback;
        }
    }

    /**
     * Enriches every element of {@code root.<arrayField>}: collects each element's
     * {@code idField} value, batch-fetches them via {@code fetcher}, then applies
     * {@code merger} to each element whose id matched a fetched result.
     */
    public Mono<String> enrichArray(String body, String arrayField, String idField,
            Function<Set<String>, Mono<Map<String, JsonNode>>> fetcher,
            BiConsumer<ObjectNode, JsonNode> merger, String context) {
        JsonNode root = readTree(body, context);
        if (root == null) {
            return Mono.just(body);
        }
        JsonNode arrayNode = root.path(arrayField);
        if (!arrayNode.isArray() || arrayNode.isEmpty()) {
            return Mono.just(body);
        }
        ArrayNode items = (ArrayNode) arrayNode;

        Set<String> ids = new LinkedHashSet<>();
        for (JsonNode item : items) {
            String id = item.path(idField).asText(null);
            if (id != null) {
                ids.add(id);
            }
        }
        if (ids.isEmpty()) {
            return Mono.just(body);
        }

        return fetcher.apply(ids).map(byId -> {
            for (JsonNode item : items) {
                if (!(item instanceof ObjectNode itemNode)) {
                    continue;
                }
                String id = item.path(idField).asText(null);
                JsonNode value = id == null ? null : byId.get(id);
                if (value != null) {
                    merger.accept(itemNode, value);
                }
            }
            return writeValueAsString(root, body, context);
        });
    }

    /**
     * Enriches the root object: reads {@code root.<idField>}, fetches it via
     * {@code fetcher}, then applies {@code merger} if a matching result was found.
     */
    public Mono<String> enrichSingle(String body, String idField,
            Function<Set<String>, Mono<Map<String, JsonNode>>> fetcher,
            BiConsumer<ObjectNode, JsonNode> merger, String context) {
        JsonNode root = readTree(body, context);
        if (!(root instanceof ObjectNode rootNode)) {
            return Mono.just(body);
        }
        String id = root.path(idField).asText(null);
        if (id == null) {
            return Mono.just(body);
        }

        return fetcher.apply(Set.of(id)).map(byId -> {
            JsonNode value = byId.get(id);
            if (value != null) {
                merger.accept(rootNode, value);
            }
            return writeValueAsString(root, body, context);
        });
    }
}
