package com.citt.rabbitadmin.dto;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

class DtoValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void cola_conNombreVacio_esInvalida() {
        assertFalse(validator.validate(new QueueRequest("", null, null, null, null, null)).isEmpty());
        assertFalse(validator.validate(new QueueRequest("   ", null, null, null, null, null)).isEmpty());
        assertFalse(validator.validate(new QueueRequest(null, null, null, null, null, null)).isEmpty());
    }

    @Test
    void cola_conCaracteresRaros_esInvalida() {
        assertFalse(validator.validate(new QueueRequest("mi cola/../x", null, null, null, null, null)).isEmpty());
    }

    @Test
    void cola_conTtlONegativo_esInvalida() {
        assertFalse(validator.validate(new QueueRequest("q1", null, 0L, null, null, null)).isEmpty());
        assertFalse(validator.validate(new QueueRequest("q1", null, null, -5, null, null)).isEmpty());
    }

    @Test
    void cola_correcta_esValida() {
        assertTrue(validator.validate(
                new QueueRequest("orders.queue", true, 30000L, 1000, "orders.dlx", "order.dead")).isEmpty());
    }

    @Test
    void exchange_sinTipo_esInvalido() {
        assertFalse(validator.validate(new ExchangeRequest("ex1", null, true)).isEmpty());
    }

    @Test
    void binding_sinCola_esInvalido() {
        assertFalse(validator.validate(new BindingRequest("", "ex1", "rk")).isEmpty());
    }

    @Test
    void binding_conWildcardsTopic_esValido() {
        assertTrue(validator.validate(new BindingRequest("q1", "ex1", "order.*.created")).isEmpty());
    }
}
