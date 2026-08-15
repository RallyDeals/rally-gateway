package com.rally.gateway.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Gateway settings.
 *
 * <p>Routed under {@code rally.gateway.*}. Headers must match the identity contract
 * documented in {@code groupdeal-architecture.md} and the catalog README: the gateway
 * validates the JWT and injects {@code X-User-Id} (= JWT {@code sub}) and
 * {@code X-User-Role} (= JWT roles) downstream. Public paths (gap A15: {@code /auth/**})
 * are skipped by the JWT filter so nobody gets locked out of login/register/refresh.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "rally.gateway")
public class GatewayProperties {

    /** Paths that bypass JWT validation (gap A15). Exact prefixes, matched with startsWith. */
    private List<String> publicPaths = List.of("/auth/login", "/auth/register", "/auth/refresh");

    private String authorizationHeader = "Authorization";
    private String bearerPrefix = "Bearer ";
    private String userIdHeader = "X-User-Id";
    private String userRoleHeader = "X-User-Role";
    private String userNameHeader = "X-User-Name";
}
