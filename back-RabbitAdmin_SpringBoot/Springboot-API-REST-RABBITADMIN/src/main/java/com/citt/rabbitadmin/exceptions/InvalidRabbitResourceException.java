package com.citt.rabbitadmin.exceptions;

/** La peticion es sintacticamente valida pero la configuracion pedida no es aceptable (HTTP 400). */
public class InvalidRabbitResourceException extends RuntimeException {

    public InvalidRabbitResourceException(String message) {
        super(message);
    }
}
