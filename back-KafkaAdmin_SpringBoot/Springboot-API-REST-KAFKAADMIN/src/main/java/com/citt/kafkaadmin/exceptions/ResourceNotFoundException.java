package com.citt.kafkaadmin.exceptions;

/** El recurso pedido (por ejemplo un consumer group) no existe en el cluster (HTTP 404). */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
