package com.citt.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/** Datos para crear un exchange. */
public record ExchangeRequest(
        @NotBlank(message = "El nombre del exchange es obligatorio")
        @Pattern(regexp = RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE)
        String name,

        @NotNull(message = "El tipo de exchange es obligatorio (DIRECT, FANOUT, TOPIC o HEADERS)")
        ExchangeType type,

        Boolean durable) {

    public boolean durableOrDefault() {
        return durable == null || durable;
    }
}
