package com.citt.rabbitadmin.exceptions;

/** La cola (u otro recurso) no existe en el broker (HTTP 404). */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
