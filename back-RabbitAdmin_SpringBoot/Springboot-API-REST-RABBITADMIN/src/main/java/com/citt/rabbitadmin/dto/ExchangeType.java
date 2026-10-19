package com.citt.rabbitadmin.dto;

/** Tipos de exchange soportados por RabbitMQ. */
public enum ExchangeType {
    DIRECT,
    FANOUT,
    TOPIC,
    HEADERS
}
