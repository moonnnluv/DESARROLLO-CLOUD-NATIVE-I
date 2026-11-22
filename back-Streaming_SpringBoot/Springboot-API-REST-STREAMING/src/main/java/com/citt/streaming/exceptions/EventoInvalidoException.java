package com.citt.streaming.exceptions;

/**
 * El evento llego bien deserializado pero sus datos no sirven para el negocio (campos obligatorios vacios).
 * No tiene sentido reintentar: el mensaje seguira siendo invalido, por eso el manejador de errores del
 * consumidor lo envia directo al Dead Letter Topic.
 */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String message) {
        super(message);
    }
}
