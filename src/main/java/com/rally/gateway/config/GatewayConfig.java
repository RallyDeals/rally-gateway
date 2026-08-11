package com.rally.gateway.config;

import com.rally.security.JwtProperties;
import com.rally.security.JwtService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Reuses rally-security's {@link JwtService} for validation without pulling in its
 * servlet-based auto-configuration (the gateway is a WebFlux application).
 *
 * <p>The JWT is signed HMAC (not encrypted) with the same {@code rally.jwt.secret}
 * every service shares, so tokens issued by the Auth Service validate here.
 *
 * <p>JWT support moved out of {@code rally-common} into the dedicated
 * {@code rally-security} package; rally-common still provides the exception
 * vocabulary ({@code ErrorResponse}, {@code UnauthenticatedException}) the filters use.
 */
@Configuration
@EnableConfigurationProperties({JwtProperties.class, GatewayProperties.class})
public class GatewayConfig {

    @Bean
    public JwtService jwtService(JwtProperties properties) {
        return new JwtService(properties);
    }
}
