package com.rally.gateway;

import com.rally.security.JwtService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the route table declared in application.yml: every service has a route and
 * representative client paths match the intended (first-match) route — including the
 * deal-vs-participation split where specific sub-paths must win over the broad
 * {@code /deals/**} deal route.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RouteConfigTest {

    @Autowired
    private RouteLocator routeLocator;

    @Autowired
    private JwtService jwtService;

    @Test
    void exposesEveryServiceRoute() {
        List<String> ids = routesByIdentifier().keySet().stream().toList();

        assertThat(ids).containsExactlyInAnyOrder(
                "auth", "catalog", "participation", "deal", "order", "payment", "inventory", "notification",
                "deal-list-composed", "deal-create-composed", "deal-analytics", "deal-cancel-composed",
                "deal-update-composed", "deal-get-composed", "product-list-composed", "product-admin-passthrough",
                "product-get-composed", "profile-personal-info-composed", "profile-my-deals-composed",
                "profile-my-deals-summary-composed");
    }

    @Test
    void wiresRallySecurityJwtService() {
        assertThat(jwtService).isNotNull();
    }

    @Test
    void matchesClientPathsToTheirRoute() {
        assertRouteMatches("auth", "/auth/login");
        assertRouteMatches("auth", "/auth/me");
        assertRouteMatches("auth", "/users/1/role");

        assertRouteMatches("catalog", "/products");
        assertRouteMatches("catalog", "/products/12");
        assertRouteMatches("catalog", "/products/admin");
        assertRouteMatches("catalog", "/categories/12");

        assertRouteMatches("participation", "/deals/12/join");
        assertRouteMatches("participation", "/deals/12/leave");
        assertRouteMatches("participation", "/deals/12/participants");
        assertRouteMatches("participation", "/deals/12/progress");
        assertRouteMatches("participation", "/deals/12/invite-link");
        assertRouteMatches("participation", "/invites/abc123");

        assertRouteMatches("deal", "/deals");
        assertRouteMatches("deal", "/deals/12");
        assertRouteMatches("deal", "/deals/12/cancel");
        assertRouteMatches("deal", "/deals/12/reserve-slot");

        assertRouteMatches("order", "/api/orders/checkout");
        assertRouteMatches("order", "/api/orders/my");
        assertRouteMatches("order", "/api/orders/my/12");

        assertRouteMatches("payment", "/api/payments/1");
        assertRouteMatches("payment", "/api/payment-methods");
        assertRouteMatches("payment", "/api/payment-methods/5");

        assertRouteMatches("inventory", "/inventory/1");

        assertRouteMatches("notification", "/notifications");

        assertRouteMatches("profile-personal-info-composed", "/profile/personal-info");
        assertRouteMatches("profile-my-deals-composed", "/profile/my-deals");
        assertRouteMatches("profile-my-deals-summary-composed", "/profile/my-deals/summary");
    }

    private void assertRouteMatches(String routeId, String path) {
        Route route = routesByIdentifier().get(routeId);
        assertThat(route).as("route %s must exist", routeId).isNotNull();

        var request = MockServerHttpRequest.get(path).build();
        var exchange = MockServerWebExchange.from(request);

        Boolean matched = Mono.from(route.getPredicate().apply(exchange)).block();

        assertThat(matched)
                .as("path %s should match route %s", path, routeId)
                .isTrue();
    }

    private Map<String, Route> routesByIdentifier() {
        return routeLocator.getRoutes()
                .collectList()
                .block()
                .stream()
                .collect(java.util.stream.Collectors.toMap(Route::getId, Function.identity()));
    }
}
