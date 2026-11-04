package com.citt.messaging.orders;

import org.springframework.stereotype.Component;

/**
 * Logica de negocio del dominio de ordenes. No sabe nada de RabbitMQ: solo valida y procesa
 * el texto de la orden, y avisa con una excepcion si algo sale mal. Decidir que hacer con
 * el mensaje (ACK, reintento o DLQ) es responsabilidad de OrderConsumer.
 */
@Component
public class OrderProcessor {

    private static final String VALID_PREFIX = "Orden ID:";
    private static final double TEMPORARY_FAILURE_RATE = 0.5;

    public void process(String message) {
        validate(message);
        simulateTemporaryFailure(message);
    }

    /** Un mensaje que no tiene el formato esperado nunca se va a poder procesar. */
    private void validate(String message) {
        if (message == null || !message.startsWith(VALID_PREFIX)) {
            throw new UnrecoverableOrderException("Formato de orden invalido: " + message);
        }
    }

    /** Simula un fallo temporal aleatorio para demostrar el reintento y la DLQ. */
    private void simulateTemporaryFailure(String message) {
        if (Math.random() < TEMPORARY_FAILURE_RATE) {
            throw new RecoverableOrderException("Fallo temporal simulado al procesar: " + message);
        }
    }
}
