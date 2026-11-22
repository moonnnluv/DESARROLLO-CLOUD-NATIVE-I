package com.citt.kafkaadmin.dto;

import java.time.Instant;

/** Respuesta uniforme para operaciones exitosas y errores. */
public record ApiMessage(Instant timestamp, int status, String message) {

    public static ApiMessage of(int status, String message) {
        return new ApiMessage(Instant.now(), status, message);
    }
}
