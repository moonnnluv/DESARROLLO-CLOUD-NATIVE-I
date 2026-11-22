package com.citt.streaming.config;

import java.time.Duration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declara los topics del pipeline con sus parametros de administracion (particiones, replicas,
 * cleanup.policy, retencion y min.insync.replicas). Spring (KafkaAdmin) los crea al iniciar si no existen.
 * Es necesario porque los brokers de docker/kafka tienen AUTO_CREATE_TOPICS_ENABLE en false.
 *
 * Justificacion de los valores:
 *  - 3 particiones: hasta 3 consumidores del mismo grupo leen en paralelo (una particion por consumidor).
 *  - 3 replicas: cada particion tiene una copia en cada broker (1 lider y 2 seguidores).
 *  - min.insync.replicas=2: con acks=all un mensaje se confirma cuando lo tienen 2 copias, asi se tolera
 *    la caida de 1 broker sin perder datos y sin dejar de escribir.
 *
 * Nota: KafkaAdmin no modifica la configuracion de un topic que ya existe. Para cambiar la retencion o el
 * cleanup.policy de un topic existente usa el microservicio KafkaAdmin (PATCH /api/admin/topics/{name}/config)
 * o Kafka UI; para cambiar las particiones o replicas hay que recrearlo.
 */
@Configuration
public class KafkaTopicsConfig {

    static final int PARTICIONES = 3;
    static final int REPLICAS = 3;
    static final String MIN_ISR = "2";

    // Retencion segun el uso de cada flujo (cleanup.policy=delete elimina segmentos mas antiguos que esto).
    private static final Duration RETENCION_VENTAS = Duration.ofDays(7);   // permite reprocesar una semana
    private static final Duration RETENCION_ALERTAS = Duration.ofDays(30); // las alertas se revisan despues
    private static final Duration RETENCION_DLT = Duration.ofDays(14);     // tiempo para revisar mensajes fallidos

    @Bean
    public NewTopic topicEntrada(PipelineProperties props) {
        return crearConRetencion(props.topics().entrada(), RETENCION_VENTAS);
    }

    @Bean
    public NewTopic topicEnriquecidas(PipelineProperties props) {
        return crearConRetencion(props.topics().enriquecidas(), RETENCION_VENTAS);
    }

    @Bean
    public NewTopic topicAlertas(PipelineProperties props) {
        return crearConRetencion(props.topics().alertas(), RETENCION_ALERTAS);
    }

    /**
     * La clave de este topic es la fecha y el valor es el total acumulado del dia. Con compact Kafka
     * conserva solo el ultimo total de cada fecha, que es lo unico que interesa consultar.
     */
    @Bean
    public NewTopic topicTotales(PipelineProperties props) {
        return base(props.topics().totales())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .build();
    }

    /** Dead Letter Topic: aqui terminan los mensajes que los consumidores no lograron procesar. */
    @Bean
    public NewTopic topicDeadLetter(@Value("${pipeline.dlt-topic:ventas-dlt}") String nombre) {
        return crearConRetencion(nombre, RETENCION_DLT);
    }

    private NewTopic crearConRetencion(String nombre, Duration retencion) {
        return base(nombre)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retencion.toMillis()))
                .build();
    }

    /** Parte comun de todos los topics: particiones, replicas y min.insync.replicas. */
    private TopicBuilder base(String nombre) {
        return TopicBuilder.name(nombre)
                .partitions(PARTICIONES)
                .replicas(REPLICAS)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_ISR);
    }
}
