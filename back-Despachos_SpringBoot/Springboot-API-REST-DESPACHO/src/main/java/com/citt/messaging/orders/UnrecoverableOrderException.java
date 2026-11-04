package com.citt.messaging.orders;

/**
 * Fallo permanente: el mensaje esta mal formado y reintentarlo no lo arregla.
 * El consumidor lo trata como no recuperable y lo envia directo a la DLQ.
 */
public class UnrecoverableOrderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UnrecoverableOrderException(String message) {
        super(message);
    }
}
