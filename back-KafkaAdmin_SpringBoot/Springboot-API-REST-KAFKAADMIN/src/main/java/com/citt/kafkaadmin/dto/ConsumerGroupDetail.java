package com.citt.kafkaadmin.dto;

import java.util.List;

/**
 * Detalle de un consumer group: miembros con sus particiones asignadas y el lag de cada particion.
 *
 * El lag es la diferencia entre el ultimo mensaje del topico (endOffset) y el ultimo que el grupo
 * confirmo haber procesado (currentOffset). Un lag que crece indica que los consumidores no dan abasto.
 */
public record ConsumerGroupDetail(
        String groupId,
        String state,
        List<MemberInfo> members,
        List<PartitionLag> lag,
        long totalLag) {

    public record MemberInfo(String clientId, String host, List<String> assignedPartitions) {
    }

    public record PartitionLag(String topic, int partition, long currentOffset, long endOffset, long lag) {
    }
}
