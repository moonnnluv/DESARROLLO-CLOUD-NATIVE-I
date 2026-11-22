package com.citt.kafkaadmin.dto;

/** Fila del listado de topicos. */
public record TopicSummary(String name, int partitions, int replicationFactor) {
}
