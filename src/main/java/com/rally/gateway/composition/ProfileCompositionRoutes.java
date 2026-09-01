package com.rally.gateway.composition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rally.gateway.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
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
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Set;

@Configuration
@RequiredArgsConstructor
public class ProfileCompositionRoutes {

    private static final Logger log = LoggerFactory.getLogger(ProfileCompositionRoutes.class);

    private final ProfilePersonalInfoService profilePersonalInfoService;
    private final ProfileMyDealsService profileMyDealsService;
    private final ProfileMyDealsSummaryService profileMyDealsSummaryService;
    private final ObjectMapper objectMapper;
    private final GatewayProperties gatewayProperties;

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
        String userId = requireUserId(exchange);
        if (userId == null) {
            return unauthorized(exchange);
        }

        log.debug("my-info request userId={}", userId);

        return profilePersonalInfoService.buildProfileResponseFor(userId)
                .flatMap(body -> writeResponse(exchange, body))
                .onErrorResume(ex -> badGateway(exchange, "Profile composition failed for userId=" + userId, ex));
    }

    private Mono<Void> composeAndWriteDeals(ServerWebExchange exchange, GatewayFilterChain chain) {
        String userId = requireUserId(exchange);
        if (userId == null) {
            return unauthorized(exchange);
        }

        int page = profileMyDealsService.parseIntParam(exchange, "page", 1);
        int size = profileMyDealsService.parseIntParam(exchange, "size", 20);
        String participationStatusFilter = profileMyDealsService.parseSingleStatusFilter(exchange, "participationStatus");
        Set<String> dealStatusFilter = profileMyDealsService.parseStatusFilter(exchange, "dealStatus");

        log.debug("my-deals request userId={} page={} size={} participationStatusFilter={} dealStatusFilter={}",
                userId, page, size, participationStatusFilter, dealStatusFilter);

        long start = System.currentTimeMillis();
        return profileMyDealsService.buildMyDealsResponseFor(userId, page, size, participationStatusFilter, dealStatusFilter)
                .doOnNext(body -> log.info("my-deals composed for userId={} in {}ms, returned {} deals",
                        userId, System.currentTimeMillis() - start, body.path("deals").size()))
                .flatMap(body -> writeResponse(exchange, body))
                .onErrorResume(ex -> badGateway(exchange, "Deals composition failed for userId=" + userId, ex));
    }

    private Mono<Void> composeAndWriteDealsSummary(ServerWebExchange exchange, GatewayFilterChain chain) {
        String userId = requireUserId(exchange);
        if (userId == null) {
            return unauthorized(exchange);
        }

        log.debug("my-deals summary requested for userId={}", userId);
        long start = System.currentTimeMillis();

        return profileMyDealsSummaryService.buildMyDealsSummaryFor(userId)
                .doOnNext(body -> log.info("my-deals summary composed for userId={} in {}ms: activeDealsCount={} savedAmount={}",
                        userId, System.currentTimeMillis() - start,
                        body.path("activeDealsCount").asLong(), body.path("savedAmount").asText()))
                .flatMap(body -> writeResponse(exchange, body))
                .onErrorResume(ex -> badGateway(exchange, "Deals summary composition failed for userId=" + userId, ex));
    }

    private String requireUserId(ServerWebExchange exchange) {
        String userId = exchange.getRequest().getHeaders().getFirst(gatewayProperties.getUserIdHeader());
        return (userId == null || userId.isBlank()) ? null : userId;
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }

    private Mono<Void> badGateway(ServerWebExchange exchange, String logMessage, Throwable ex) {
        log.warn("{}: {}", logMessage, ex.toString());
        exchange.getResponse().setStatusCode(HttpStatus.BAD_GATEWAY);
        return exchange.getResponse().setComplete();
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
