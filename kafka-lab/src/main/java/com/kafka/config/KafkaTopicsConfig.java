package com.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics del laboratorio declarados en codigo. El compose tiene KAFKA_AUTO_CREATE_TOPICS_ENABLE
 * en false, asi que Spring (KafkaAdmin) los crea al iniciar si no existen.
 * Equivale a los comandos kafka-topics.sh de las guias 3.2.1 y 3.3.2.
 */
@Configuration
public class KafkaTopicsConfig {

    private static final int PARTICIONES = 3;
    private static final int REPLICAS = 1; // Un solo broker de laboratorio

    /** Guia 3.3.2: retencion de 5 minutos o 50 MiB por particion, con segmentos de 1 MiB. */
    @Bean
    public NewTopic sensorTopic() {
        return TopicBuilder.name("sensor-topic")
                .partitions(PARTICIONES)
                .replicas(REPLICAS)
                .config(TopicConfig.RETENTION_MS_CONFIG, "300000")
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "52428800")
                .config(TopicConfig.SEGMENT_BYTES_CONFIG, "1048576")
                .build();
    }

    /** Guia 3.2.1: topic para observar particiones, claves y offsets. */
    @Bean
    public NewTopic tutorialParticiones() {
        return TopicBuilder.name("tutorial-particiones")
                .partitions(PARTICIONES)
                .replicas(REPLICAS)
                .build();
    }
}
