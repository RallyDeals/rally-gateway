package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Configuration
public class ProfileCompositionRoutes {

    private static final Logger log = LoggerFactory.getLogger(ProfileCompositionRoutes.class);
    private static final String USER_ID_HEADER = "X-User-Id";

    private final WebClient authServiceWebClient;
    private final WebClient participationServiceWebClient;
    private final WebClient dealServiceWebClient;
    private final WebClient orderServiceWebClient;
    private final WebClient catalogServiceWebClient;
    private final ObjectMapper objectMapper;

    public ProfileCompositionRoutes(
            @Value("${AUTH_SERVICE_URI:http://localhost:8084}") String authServiceUri,
            @Value("${PARTICIPATION_SERVICE_URI:http://localhost:8086}") String participationServiceUri,
            @Value("${DEAL_SERVICE_URI:http://localhost:8085}") String dealServiceUri,
            @Value("${ORDER_SERVICE_URI:http://localhost:8081}") String orderServiceUri,
            WebClient.Builder webClientBuilder,
            WebClient catalogServiceWebClient,
            ObjectMapper objectMapper) {
        this.authServiceWebClient = webClientBuilder.baseUrl(authServiceUri).build();
        this.participationServiceWebClient = webClientBuilder.baseUrl(participationServiceUri).build();
        this.dealServiceWebClient = webClientBuilder.baseUrl(dealServiceUri).build();
        this.orderServiceWebClient = webClientBuilder.baseUrl(orderServiceUri).build();
        this.catalogServiceWebClient = catalogServiceWebClient;
        this.objectMapper = objectMapper;
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public RouteLocator profileCompositionRouteLocator(RouteLocatorBuilder builder,
                                                       @Value("${AUTH_SERVICE_URI:http://localhost:8084}") String authServiceUri) {
        return builder.routes()
                .route("profile-personal-info-composed", r -> r.method(HttpMethod.GET)
                        .and().path("/profile/personal-info")
                        .filters(f -> f.filter(this::composeAndWriteProfile))
                        .uri(authServiceUri))
                .route("profile-my-deals-composed", r -> r.method(HttpMethod.GET)
                        .and().path("/profile/my-deals")
                        .filters(f -> f.filter(this::composeAndWriteDeals))
                        .uri(authServiceUri))
                .route("profile-my-deals-summary-composed", r -> r.method(HttpMethod.GET)
                        .and().path("/profile/my-deals/summary")
                        .filters(f -> f.filter(this::composeAndWriteDealsSummary))
                        .uri(authServiceUri))
                .build();
    }


    private Mono<Void> composeAndWriteProfile(ServerWebExchange exchange, GatewayFilterChain chain) {
        String userId = exchange.getRequest().getHeaders().getFirst(USER_ID_HEADER);
        if (userId == null || userId.isBlank()) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        log.debug("my-info request userId={}", userId);

        return buildProfileResponseFor(userId)
                .flatMap(body -> writeResponse(exchange, body))
                .onErrorResume(ex -> {
                    log.warn("Profile composition failed for userId={}: {}", userId, ex.toString());
                    exchange.getResponse().setStatusCode(HttpStatus.BAD_GATEWAY);
                    return exchange.getResponse().setComplete();
                });
    }

    private Mono<Void> composeAndWriteDeals(ServerWebExchange exchange, GatewayFilterChain chain) {
        String userId = exchange.getRequest().getHeaders().getFirst(USER_ID_HEADER);
        if (userId == null || userId.isBlank()) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        int page = parseIntParam(exchange, "page", 1);
        int size = parseIntParam(exchange, "size", 20);
        String participationStatusFilter = parseSingleStatusFilter(exchange, "participationStatus");
        Set<String> dealStatusFilter = parseStatusFilter(exchange, "dealStatus");

        log.debug("my-deals request userId={} page={} size={} participationStatusFilter={} dealStatusFilter={}",
                userId, page, size, participationStatusFilter, dealStatusFilter);

        long start = System.currentTimeMillis();
        return buildMyDealsResponseFor(userId, page, size, participationStatusFilter, dealStatusFilter)
                .doOnNext(body -> log.info("my-deals composed for userId={} in {}ms, returned {} deals",
                        userId, System.currentTimeMillis() - start, body.path("deals").size()))
                .flatMap(body -> writeResponse(exchange, body))
                .onErrorResume(ex -> {
                    log.warn("Deals composition failed for userId={}: {}", userId, ex.toString());
                    exchange.getResponse().setStatusCode(HttpStatus.BAD_GATEWAY);
                    return exchange.getResponse().setComplete();
                });
    }

    private Mono<Void> composeAndWriteDealsSummary(ServerWebExchange exchange, GatewayFilterChain chain) {
        String userId = exchange.getRequest().getHeaders().getFirst(USER_ID_HEADER);
        if (userId == null || userId.isBlank()) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        log.debug("my-deals summary requested for userId={}", userId);
        long start = System.currentTimeMillis();

        return buildMyDealsSummaryFor(userId)
                .doOnNext(body -> log.info("my-deals summary composed for userId={} in {}ms: activeDealsCount={} savedAmount={}",
                        userId, System.currentTimeMillis() - start,
                        body.path("activeDealsCount").asLong(), body.path("savedAmount").asText()))
                .flatMap(body -> writeResponse(exchange, body))
                .onErrorResume(ex -> {
                    log.warn("Deals summary composition failed for userId={}: {}", userId, ex.toString());
                    exchange.getResponse().setStatusCode(HttpStatus.BAD_GATEWAY);
                    return exchange.getResponse().setComplete();
                });
    }

    private int parseIntParam(ServerWebExchange exchange, String paramName, int defaultValue) {
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

    private Set<String> parseStatusFilter(ServerWebExchange exchange, String paramName) {
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

    private String parseSingleStatusFilter(ServerWebExchange exchange, String paramName) {
        String raw = exchange.getRequest().getQueryParams().getFirst(paramName);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return raw.trim().toUpperCase(Locale.ROOT);
    }

    private Mono<JsonNode> buildProfileResponseFor(String userId) {
        Mono<JsonNode> profileInfoMono = authServiceWebClient.get()
                .uri("/auth/me")
                .header(USER_ID_HEADER, userId)
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
                .header(USER_ID_HEADER, userId)
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
                .header(USER_ID_HEADER, userId)
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

    private Mono<JsonNode> buildMyDealsResponseFor(String userId, int page, int size,
            String participationStatusFilter, Set<String> dealStatusFilter) {
        Mono<JsonNode> participationsMono = fetchParticipationsPage(userId, page, size);

        return participationsMono.flatMap(root -> joinedDealsBulk(root)
                .flatMap(this::enrichWithProducts)
                .map(deals -> buildMyDealsResponse(deals, root, participationStatusFilter, dealStatusFilter)));
    }

    private Mono<JsonNode> fetchParticipationsPage(String userId, int page, int size) {
        return participationServiceWebClient.get()
                .uri(uriBuilder -> uriBuilder.path("/participations")
                        .queryParam("userId", userId)
                        .queryParam("page", page)
                        .queryParam("size", size)
                        .build())
                .header(USER_ID_HEADER, userId)
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

    private Mono<JsonNode> buildMyDealsSummaryFor(String userId) {
        return fetchAllParticipations(userId)
                .flatMap(this::bulkFetchDealsFor)
                .map(this::buildMyDealsSummaryResponse);
    }

    private static final int SUMMARY_PAGE_SIZE = 100;
    private static final int SUMMARY_PAGE_CONCURRENCY = 4;

    private Mono<List<JsonNode>> fetchAllParticipations(String userId) {
        return fetchParticipationsPage(userId, 1, SUMMARY_PAGE_SIZE)
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
                            .flatMap(page -> fetchParticipationsPage(userId, page, SUMMARY_PAGE_SIZE),
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
        return bulkFetchDeals(dealIds);
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

    private Mono<JsonNode> enrichWithProducts(JsonNode deals) {
        if (!deals.isArray() || deals.isEmpty()) {
            return Mono.just(deals);
        }

        Set<String> productIds = new LinkedHashSet<>();
        for (JsonNode deal : deals) {
            String productId = deal.path("productId").asText(null);
            if (productId != null) {
                productIds.add(productId);
            }
        }
        if (productIds.isEmpty()) {
            return Mono.just(deals);
        }

        return fetchProducts(productIds).map(products -> {
            for (JsonNode deal : deals) {
                if (!(deal instanceof ObjectNode dealNode)) {
                    continue;
                }
                String productId = deal.path("productId").asText(null);
                JsonNode product = productId == null ? null : products.get(productId);
                if (product != null) {
                    mergeProductFields(dealNode, product);
                }
            }
            return deals;
        });
    }

    private Mono<Map<String, JsonNode>> fetchProducts(Set<String> productIds) {
        log.debug("fetching {} products from catalog service", productIds.size());
        return Flux.fromIterable(productIds)
                .flatMap(id -> catalogServiceWebClient.get()
                        .uri("/internal/products/{id}", id)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .map(node -> new SimpleEntry<>(id, node))
                        .onErrorResume(ex -> {
                            log.warn("Product lookup failed for productId={}: {}", id, ex.toString());
                            return Mono.empty();
                        }))
                .collectMap(SimpleEntry::getKey, SimpleEntry::getValue)
                .doOnNext(map -> log.debug("resolved {}/{} products from catalog", map.size(), productIds.size()));
    }

    private void mergeProductFields(ObjectNode dealNode, JsonNode product) {
        dealNode.put("productName", product.path("productName").asText(null));
        dealNode.put("productImageUrl", product.path("productImageUrl").asText(null));
        dealNode.put("category", product.path("category").path("name").asText(null));
        dealNode.put("sku", product.path("sku").asText(null));
        dealNode.put("sellerName", product.path("sellerName").asText(null));
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
        return bulkFetchDeals(dealIds);
    }

    private Mono<JsonNode> bulkFetchDeals(Set<String> dealIds) {
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

    private JsonNode buildProfileResponse(JsonNode profileInfo, long joinedCount, long orderCount) {
        ObjectNode response = objectMapper.createObjectNode();
        response.set("info", profileInfo);
        response.put("dealsJoinedCount", joinedCount);
        response.put("ordersCount", orderCount);
        log.info("Profile response built for user {}", profileInfo.path("id").asText());
        return response;
    }

    private Mono<Void> writeResponse(ServerWebExchange exchange, JsonNode body) {
        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(body);
        } catch (Exception ex) {
            return Mono.error(ex);
        }
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
