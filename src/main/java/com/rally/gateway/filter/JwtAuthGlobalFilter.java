package com.rally.gateway.filter;

import com.rally.common.exceptions.shared.UnauthenticatedException;
import com.rally.gateway.config.GatewayProperties;
import com.rally.security.JwtService;
import io.jsonwebtoken.Claims;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Validates the {@code Authorization: Bearer <JWT>} header on every request except the
 * public paths (gap A15), then forwards identity downstream:
 *
 * <ul>
 *   <li>strips the raw token — services never see a JWT or the client's identity headers;</li>
 *   <li>injects {@code X-User-Id}  = JWT {@code sub};</li>
 *   <li>injects {@code X-User-Role} = JWT {@code roles} (comma-joined).</li>
 * </ul>
 *
 * <p>Client-supplied {@code X-User-Id}/{@code X-User-Role} are always removed first so
 * they cannot be spoofed (the whole trust model relies on these coming only from the
 * gateway). A missing/invalid/expired token on a protected path yields a 401 in the same
 * {@code ErrorResponse} shape rally-common returns service-side (written via
 * {@link ErrorResponseWriter}).
 */
@Component
public class JwtAuthGlobalFilter implements GlobalFilter, Ordered {

    private final JwtService jwtService;
    private final GatewayProperties properties;
    private final ErrorResponseWriter errorResponseWriter;

    public JwtAuthGlobalFilter(JwtService jwtService, GatewayProperties properties, ErrorResponseWriter errorResponseWriter) {
        this.jwtService = jwtService;
        this.properties = properties;
        this.errorResponseWriter = errorResponseWriter;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        if (isPublic(path)) {
            return chain.filter(stripIdentityHeaders(exchange));
        }

        String token = extractToken(exchange);
        if (token == null) {
            return writeUnauthorized(exchange, "Missing bearer token");
        }

        final Claims claims;
        try {
            claims = jwtService.parseAndValidate(token);
        } catch (UnauthenticatedException ex) {
            return writeUnauthorized(exchange, ex.getMessage());
        } catch (Exception ex) {
            return writeUnauthorized(exchange, "Invalid token");
        }

        String userId = jwtService.getUserId(claims);
        List<String> roles = jwtService.getRoles(claims);
        String username = jwtService.getUsername(claims);

        ServerWebExchange authenticated = exchange.mutate()
                .request(request -> request.headers(headers -> {
                    headers.remove(properties.getAuthorizationHeader());
                    headers.remove(properties.getUserIdHeader());
                    headers.remove(properties.getUserRoleHeader());
                    headers.set(properties.getUserIdHeader(), userId);
                    if (!roles.isEmpty()) {
                        headers.set(properties.getUserRoleHeader(), String.join(",", roles));
                    }
                    if (username != null && !username.isEmpty()) {
                        headers.set(properties.getUserNameHeader(), username);
                    }
                }))
                .build();

        return chain.filter(authenticated);
    }

    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

    private boolean isPublic(String path) {
        return properties.getPublicPaths().stream().anyMatch(pattern -> PATH_MATCHER.match(pattern, path));
    }

    /** Remove identity headers (including the raw token) on public paths before forwarding. */
    private ServerWebExchange stripIdentityHeaders(ServerWebExchange exchange) {
        return exchange.mutate()
                .request(request -> request.headers(headers -> {
                    headers.remove(properties.getAuthorizationHeader());
                    headers.remove(properties.getUserIdHeader());
                    headers.remove(properties.getUserRoleHeader());
                    headers.remove(properties.getUserNameHeader());
                }))
                .build();
    }

    private String extractToken(ServerWebExchange exchange) {
        String header = exchange.getRequest().getHeaders().getFirst(properties.getAuthorizationHeader());
        if (header != null && header.startsWith(properties.getBearerPrefix())) {
            return header.substring(properties.getBearerPrefix().length()).trim();
        }
        return null;
    }

    private Mono<Void> writeUnauthorized(ServerWebExchange exchange, String message) {
        return errorResponseWriter.write(exchange, HttpStatus.UNAUTHORIZED, "Unauthenticated", message);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
