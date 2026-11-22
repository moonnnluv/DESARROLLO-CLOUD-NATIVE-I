package com.citt.kafkaadmin.exceptions;

/** El cluster no respondio a tiempo o la operacion fue interrumpida (HTTP 502). */
public class KafkaUnavailableException extends RuntimeException {

    public KafkaUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
