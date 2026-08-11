package com.rally.gateway.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rally.common.exceptions.handler.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Shared writer that turns an error into the standard rally-common
 * {@link ErrorResponse} JSON body and writes it to a reactive exchange.
 *
 * <p>Both global filters used to duplicate this (JwtAuthGlobalFilter's 401 helper and
 * FallbackGlobalFilter's 503 helper were identical except for the status/title/message),
 * so the common part lives here once. The {@link ErrorResponse} DTO is the piece that
 * comes from {@code rally-common}; the serializing/writing is WebFlux-specific, so it
 * stays in the gateway rather than the servlet-oriented common library.
 */
@Component
public class ErrorResponseWriter {

    private final ObjectMapper objectMapper;

    public ErrorResponseWriter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Mono<Void> write(ServerWebExchange exchange, HttpStatus status, String title, String message) {
        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(status.value())
                .title(title)
                .message(message)
                .path(exchange.getRequest().getURI().getPath())
                .build();

        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        try {
            byte[] body = objectMapper.writeValueAsBytes(error);
            return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
        } catch (JsonProcessingException ex) {
            return exchange.getResponse().setComplete();
        }
    }
}
