package com.citt.messaging.orders;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class OrderProcessorTest {

    private final OrderProcessor processor = new OrderProcessor();

    @Test
    void mensajeSinFormatoDeOrdenEsNoRecuperable() {
        assertThrows(UnrecoverableOrderException.class, () -> processor.process("mensaje corrupto"));
    }

    @Test
    void mensajeNuloEsNoRecuperable() {
        assertThrows(UnrecoverableOrderException.class, () -> processor.process(null));
    }
}
