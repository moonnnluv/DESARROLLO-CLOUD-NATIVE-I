package com.citt.messaging.orders;

/**
 * Fallo temporal al procesar una orden (por ejemplo, un recurso externo que no respondio).
 * El consumidor lo trata como recuperable: reintenta una vez y, si vuelve a fallar, va a la DLQ.
 */
public class RecoverableOrderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RecoverableOrderException(String message) {
        super(message);
    }
}
