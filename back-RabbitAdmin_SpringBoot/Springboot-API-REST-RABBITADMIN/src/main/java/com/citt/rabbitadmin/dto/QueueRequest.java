package com.citt.rabbitadmin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Datos para crear una cola. Los campos opcionales permiten configurar TTL,
 * largo maximo y Dead Letter Exchange (DLX) como en RabbitMQConfig de Ventas y Despachos.
 */
public record QueueRequest(
        @NotBlank(message = "El nombre de la cola es obligatorio")
        @Pattern(regexp = RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE)
        String name,

        Boolean durable,

        @Min(value = 1, message = "messageTtlMs debe ser mayor que 0")
        Long messageTtlMs,

        @Min(value = 1, message = "maxLength debe ser mayor que 0")
        Integer maxLength,

        @Pattern(regexp = "^$|" + RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE)
        String deadLetterExchange,

        @Pattern(regexp = "^$|" + RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE)
        String deadLetterRoutingKey) {

    /** Por defecto las colas son durables. */
    public boolean durableOrDefault() {
        return durable == null || durable;
    }
}
