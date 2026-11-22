package com.citt.streaming.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

/**
 * Verifica los parametros de administracion de los topics sin necesitar Kafka:
 * 3 particiones, 3 replicas, min.insync.replicas=2 y la politica de limpieza de cada flujo.
 */
class KafkaTopicsConfigTest {

    private static final PipelineProperties PROPS = new PipelineProperties(
            new PipelineProperties.Topics("ventas-registradas", "ventas-enriquecidas",
                    "ventas-alertas", "ventas-totales-dia"),
            new PipelineProperties.Reglas(1_000, 500_000),
            new PipelineProperties.Conversion(950.0));

    private final KafkaTopicsConfig config = new KafkaTopicsConfig();

    @Test
    void todosLosTopics_tienenTresParticionesTresReplicasYMinIsrDos() {
        NewTopic[] topics = {
                config.topicEntrada(PROPS),
                config.topicEnriquecidas(PROPS),
                config.topicAlertas(PROPS),
                config.topicTotales(PROPS),
                config.topicDeadLetter("ventas-dlt")
        };

        for (NewTopic topic : topics) {
            assertEquals(3, topic.numPartitions(), topic.name());
            assertEquals((short) 3, topic.replicationFactor(), topic.name());
            assertEquals("2", topic.configs().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), topic.name());
        }
    }

    @Test
    void topicsDeVentasYAlertas_usanCleanupDeleteConSuRetencion() {
        NewTopic entrada = config.topicEntrada(PROPS);
        NewTopic alertas = config.topicAlertas(PROPS);

        assertEquals("delete", entrada.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("604800000", entrada.configs().get(TopicConfig.RETENTION_MS_CONFIG));   // 7 dias
        assertEquals("2592000000", alertas.configs().get(TopicConfig.RETENTION_MS_CONFIG));  // 30 dias
    }

    @Test
    void topicDeTotales_usaCleanupCompact() {
        NewTopic totales = config.topicTotales(PROPS);

        assertEquals("compact", totales.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));
    }

    @Test
    void deadLetterTopic_conservaLosMensajesFallidosCatorceDias() {
        NewTopic dlt = config.topicDeadLetter("ventas-dlt");

        assertEquals("ventas-dlt", dlt.name());
        assertEquals("1209600000", dlt.configs().get(TopicConfig.RETENTION_MS_CONFIG));
    }
}
