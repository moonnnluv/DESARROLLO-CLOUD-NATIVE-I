package com.citt.rabbitadmin.exceptions;

/** El recurso que se intenta crear ya existe (HTTP 409). */
public class ResourceAlreadyExistsException extends RuntimeException {

    public ResourceAlreadyExistsException(String message) {
        super(message);
    }
}
