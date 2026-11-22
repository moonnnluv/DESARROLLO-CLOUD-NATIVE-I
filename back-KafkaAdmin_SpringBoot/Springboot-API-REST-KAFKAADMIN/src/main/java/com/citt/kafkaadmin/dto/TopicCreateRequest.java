package com.citt.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

/**
 * Datos para crear un topico. Particiones y replicas son opcionales y, si faltan, se usan
 * los valores del proyecto (3 y 3). Las configuraciones son las propias de Kafka, por ejemplo
 * retention.ms, cleanup.policy o min.insync.replicas.
 */
public record TopicCreateRequest(
        @NotBlank(message = "El nombre del topico es obligatorio")
        @Pattern(regexp = KafkaNames.TOPIC_PATTERN, message = KafkaNames.TOPIC_PATTERN_MESSAGE)
        String name,

        @Min(value = 1, message = "partitions debe ser al menos 1")
        @Max(value = KafkaNames.MAX_PARTITIONS, message = "partitions no puede superar 1000")
        Integer partitions,

        @Min(value = 1, message = "replicationFactor debe ser al menos 1")
        @Max(value = KafkaNames.MAX_REPLICATION_FACTOR, message = "replicationFactor no puede superar 10")
        Integer replicationFactor,

        Map<String, String> configs) {

    public int partitionsOrDefault() {
        return partitions == null ? KafkaNames.DEFAULT_PARTITIONS : partitions;
    }

    public short replicationFactorOrDefault() {
        return (short) (replicationFactor == null ? KafkaNames.DEFAULT_REPLICATION_FACTOR : replicationFactor);
    }

    public Map<String, String> configsOrEmpty() {
        return configs == null ? Map.of() : configs;
    }
}
