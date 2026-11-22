package com.citt.kafkaadmin.dto;

import java.util.List;

/** Estado general del cluster: identificador, broker controlador y brokers conectados. */
public record ClusterInfo(String clusterId, int controllerId, List<BrokerInfo> brokers) {

    public record BrokerInfo(int id, String host, int port) {
    }
}
