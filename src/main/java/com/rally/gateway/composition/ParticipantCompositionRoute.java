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

/**
 * Response composition for {@code GET /deals/{dealId}/participants}: the route proxies
 * normally to the Participation Service (same as a plain declarative route, so JWT
 * auth/rate limiting/etc. from the gateway's usual filter chain still apply), then a
 * {@code modifyResponseBody} filter fetches the participants' user info from the Auth
 * Service in a single batched call and merges it in.
 *
 * <p>{@code @Order(HIGHEST_PRECEDENCE)} makes sure this route is matched before the
 * declarative {@code participation} route in application.yml (Path=
 * {@code /deals/*&#47;participants}), which would otherwise win first for the same path
 * and return the raw, unenriched response.
 */
@Configuration
@RequiredArgsConstructor
public class ParticipantCompositionRoute {

    private static final Logger log = LoggerFactory.getLogger(ParticipantCompositionRoute.class);
    private static final String CONTEXT = "participation-service";

    private final WebClient authServiceWebClient;
    private final ObjectMapper objectMapper;
    private final JsonRewriteSupport jsonRewriteSupport;

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public RouteLocator participantCompositionRouteLocator(RouteLocatorBuilder builder,
            @Value("${PARTICIPATION_SERVICE_URI:http://localhost:8086}") String participationServiceUri) {
        RewriteFunction<String, String> enrichPage = (exchange, body) -> enrichPage(body);

        return builder.routes()
                .route("participant-composition", r -> r.method(HttpMethod.GET)
                        .and().path("/deals/{dealId}/participants")
                        .filters(f -> f.modifyResponseBody(String.class, String.class, enrichPage))
                        .uri(participationServiceUri))
                .build();
    }

    private Mono<String> enrichPage(String body) {
        return jsonRewriteSupport.enrichArray(body, "participants", "userId",
                this::fetchUsers, this::mergeUserFields, CONTEXT);
    }

    private Mono<Map<String, JsonNode>> fetchUsers(Set<String> userIds) {
        ArrayNode ids = objectMapper.createArrayNode();
        userIds.forEach(ids::add);

        return authServiceWebClient.method(HttpMethod.GET)
                .uri("/users/batch")
                .bodyValue(ids)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(this::indexUsersById)
                .onErrorResume(ex -> {
                    log.warn("User batch lookup failed for {} ids: {}", userIds.size(), ex.toString());
                    return Mono.just(Map.of());
                });
    }

    private Map<String, JsonNode> indexUsersById(JsonNode users) {
        Map<String, JsonNode> usersById = new HashMap<>();
        for (JsonNode user : users) {
            usersById.put(user.path("id").asText(), user);
        }
        return usersById;
    }

    private void mergeUserFields(ObjectNode participantNode, JsonNode user) {
        participantNode.put("firstName", user.path("firstName").asText(null));
        participantNode.put("lastName", user.path("lastName").asText(null));
        participantNode.put("email", user.path("email").asText(null));
    }
}
