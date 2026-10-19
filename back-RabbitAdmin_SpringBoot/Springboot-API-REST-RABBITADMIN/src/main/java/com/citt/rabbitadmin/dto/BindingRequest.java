package com.citt.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/** Datos para enlazar una cola a un exchange con una routing key. */
public record BindingRequest(
        @NotBlank(message = "La cola es obligatoria")
        @Pattern(regexp = RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE)
        String queue,

        @NotBlank(message = "El exchange es obligatorio")
        @Pattern(regexp = RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE)
        String exchange,

        // En un exchange fanout la routing key se ignora, por eso puede ir vacia
        @Pattern(regexp = "^[A-Za-z0-9._*#-]{0,255}$",
                message = "La routing key solo admite letras, numeros, '.', '-', '_', '*' y '#'")
        String routingKey) {

    public String routingKeyOrEmpty() {
        return routingKey == null ? "" : routingKey;
    }
}
