package com.citt.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/**
 * Detalle de un topico.
 *
 * @param configs solo las configuraciones definidas a nivel de topico (las que difieren del broker),
 *                por ejemplo retention.ms o cleanup.policy
 */
public record TopicDetail(
        String name,
        int partitions,
        int replicationFactor,
        List<PartitionDetail> partitionDetails,
        Map<String, String> configs) {

    /**
     * Estado de una particion: broker lider, brokers con copia (replicas) y brokers al dia con el lider (ISR).
     * Si el ISR tiene menos brokers que las replicas, alguna copia se quedo atrasada o su broker esta caido.
     */
    public record PartitionDetail(int partition, int leader, List<Integer> replicas, List<Integer> isr) {
    }
}
