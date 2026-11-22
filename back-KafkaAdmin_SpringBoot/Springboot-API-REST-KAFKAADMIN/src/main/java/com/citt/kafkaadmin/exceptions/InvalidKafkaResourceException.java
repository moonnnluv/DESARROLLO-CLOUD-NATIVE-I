package com.citt.kafkaadmin.exceptions;

/** La peticion es sintacticamente valida pero la configuracion pedida no es aceptable (HTTP 400). */
public class InvalidKafkaResourceException extends RuntimeException {

    public InvalidKafkaResourceException(String message) {
        super(message);
    }
}
