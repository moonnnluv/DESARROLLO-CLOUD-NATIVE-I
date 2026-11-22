package com.citt.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Nuevo total de particiones de un topico. Kafka solo permite AUMENTAR el numero de particiones,
 * nunca reducirlo (reducirlo exige borrar y volver a crear el topico).
 */
public record PartitionsUpdateRequest(
        @NotNull(message = "El nuevo total de particiones es obligatorio")
        @Min(value = 1, message = "partitions debe ser al menos 1")
        @Max(value = KafkaNames.MAX_PARTITIONS, message = "partitions no puede superar 1000")
        Integer partitions) {
}
