package com.citt.streaming.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declara los topics del pipeline. Spring (KafkaAdmin) los crea al iniciar si no existen.
 * Es necesario porque el compose de kafka-lab tiene KAFKA_AUTO_CREATE_TOPICS_ENABLE en false.
 */
@Configuration
public class KafkaTopicsConfig {

    private static final int PARTICIONES = 3;
    private static final int REPLICAS = 1; // Un solo broker de laboratorio

    @Bean
    public NewTopic topicEntrada(PipelineProperties props) {
        return crear(props.topics().entrada());
    }

    @Bean
    public NewTopic topicEnriquecidas(PipelineProperties props) {
        return crear(props.topics().enriquecidas());
    }

    @Bean
    public NewTopic topicAlertas(PipelineProperties props) {
        return crear(props.topics().alertas());
    }

    @Bean
    public NewTopic topicTotales(PipelineProperties props) {
        return crear(props.topics().totales());
    }

    private NewTopic crear(String nombre) {
        return TopicBuilder.name(nombre).partitions(PARTICIONES).replicas(REPLICAS).build();
    }
}
