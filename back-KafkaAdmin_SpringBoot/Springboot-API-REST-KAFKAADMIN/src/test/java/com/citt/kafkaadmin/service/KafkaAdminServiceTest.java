package com.citt.kafkaadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.citt.kafkaadmin.dto.ClusterInfo;
import com.citt.kafkaadmin.dto.ConsumerGroupDetail;
import com.citt.kafkaadmin.dto.PartitionsUpdateRequest;
import com.citt.kafkaadmin.dto.TopicConfigUpdateRequest;
import com.citt.kafkaadmin.dto.TopicCreateRequest;
import com.citt.kafkaadmin.dto.TopicSummary;
import com.citt.kafkaadmin.exceptions.InvalidKafkaResourceException;
import com.citt.kafkaadmin.exceptions.KafkaUnavailableException;
import com.citt.kafkaadmin.exceptions.ResourceNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Pruebas del servicio sin Kafka ni Docker: el cliente Admin se reemplaza por un mock y se
 * controla lo que "responde" el cluster. Patron: Arrange (preparar), Act (llamar), Assert (verificar).
 */
class KafkaAdminServiceTest {

    private Admin admin;
    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        service = new KafkaAdminService(admin);
    }

    // ------------------------------------------------------------ crear topico

    @Test
    void createTopic_declaraParticionesReplicasYConfiguraciones() {
        clusterConBrokers(3);
        CreateTopicsResult resultado = creacionExitosa();
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        service.createTopic(new TopicCreateRequest("ventas-registradas", 3, 3,
                Map.of("retention.ms", "604800000", "min.insync.replicas", "2")));

        NewTopic topic = topicEnviado();
        assertEquals("ventas-registradas", topic.name());
        assertEquals(3, topic.numPartitions());
        assertEquals((short) 3, topic.replicationFactor());
        assertEquals("604800000", topic.configs().get("retention.ms"));
        assertEquals("2", topic.configs().get("min.insync.replicas"));
    }

    @Test
    void createTopic_sinParticionesNiReplicas_usaTresYTres() {
        clusterConBrokers(3);
        CreateTopicsResult resultado = creacionExitosa();
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        service.createTopic(new TopicCreateRequest("t1", null, null, null));

        NewTopic topic = topicEnviado();
        assertEquals(3, topic.numPartitions());
        assertEquals((short) 3, topic.replicationFactor());
    }

    @Test
    void createTopic_conMasReplicasQueBrokers_lanzaInvalido() {
        clusterConBrokers(1);

        assertThrows(InvalidKafkaResourceException.class,
                () -> service.createTopic(new TopicCreateRequest("t1", 3, 3, null)));
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void createTopic_conMinIsrMayorQueReplicas_lanzaInvalido() {
        clusterConBrokers(3);

        assertThrows(InvalidKafkaResourceException.class,
                () -> service.createTopic(new TopicCreateRequest("t1", 3, 3, Map.of("min.insync.replicas", "4"))));
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void createTopic_conNombreInterno_lanzaInvalido() {
        assertThrows(InvalidKafkaResourceException.class,
                () -> service.createTopic(new TopicCreateRequest("__consumer_offsets", 3, 3, null)));
    }

    @Test
    void createTopic_conNombreDeConfiguracionInvalido_lanzaInvalido() {
        assertThrows(InvalidKafkaResourceException.class,
                () -> service.createTopic(new TopicCreateRequest("t1", 3, 3, Map.of("Retention MS", "1"))));
    }

    @Test
    void createTopic_siYaExiste_propagaLaExcepcionDeKafka() {
        clusterConBrokers(3);
        KafkaFutureImpl<Void> fallida = new KafkaFutureImpl<>();
        fallida.completeExceptionally(new TopicExistsException("ya existe"));
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        when(resultado.all()).thenReturn(fallida);
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        assertThrows(TopicExistsException.class,
                () -> service.createTopic(new TopicCreateRequest("t1", 3, 3, null)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void createTopic_siElClusterNoResponde_lanzaNoDisponible() throws Exception {
        KafkaFuture<Collection<Node>> lento = mock(KafkaFuture.class);
        when(lento.get(anyLong(), any(TimeUnit.class))).thenThrow(new java.util.concurrent.TimeoutException());
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(cluster.nodes()).thenReturn(lento);
        when(admin.describeCluster()).thenReturn(cluster);

        assertThrows(KafkaUnavailableException.class,
                () -> service.createTopic(new TopicCreateRequest("t1", 3, 3, null)));
    }

    // ----------------------------------------------------- modificar topico

    @Test
    void updateConfigs_enviaSetParaLosValoresYDeleteParaLosQuitados() {
        AlterConfigsResult resultado = mock(AlterConfigsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.<Void>completedFuture(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(resultado);

        service.updateConfigs("ventas-alertas",
                new TopicConfigUpdateRequest(Map.of("retention.ms", "86400000"), List.of("segment.bytes")));

        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).incrementalAlterConfigs(captor.capture());
        Collection<AlterConfigOp> operaciones = captor.getValue().values().iterator().next();
        assertEquals(2, operaciones.size());
        assertTrue(operaciones.stream().anyMatch(op -> op.opType() == AlterConfigOp.OpType.SET
                && op.configEntry().name().equals("retention.ms")
                && "86400000".equals(op.configEntry().value())));
        assertTrue(operaciones.stream().anyMatch(op -> op.opType() == AlterConfigOp.OpType.DELETE
                && op.configEntry().name().equals("segment.bytes")));
    }

    @Test
    void updateConfigs_sinCambios_lanzaInvalido() {
        assertThrows(InvalidKafkaResourceException.class,
                () -> service.updateConfigs("t1", new TopicConfigUpdateRequest(null, null)));
        verify(admin, never()).incrementalAlterConfigs(anyMap());
    }

    @Test
    void updateConfigs_mismaClaveEnSetYRemove_lanzaInvalido() {
        assertThrows(InvalidKafkaResourceException.class,
                () -> service.updateConfigs("t1",
                        new TopicConfigUpdateRequest(Map.of("retention.ms", "1"), List.of("retention.ms"))));
    }

    @Test
    void increasePartitions_pideElNuevoTotal() {
        CreatePartitionsResult resultado = mock(CreatePartitionsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.<Void>completedFuture(null));
        when(admin.createPartitions(anyMap())).thenReturn(resultado);

        service.increasePartitions("t1", new PartitionsUpdateRequest(6));

        ArgumentCaptor<Map<String, NewPartitions>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).createPartitions(captor.capture());
        assertEquals(6, captor.getValue().get("t1").totalCount());
    }

    // ----------------------------------------------------- eliminar topico

    @Test
    void deleteTopic_conNombreInterno_lanzaInvalido() {
        assertThrows(InvalidKafkaResourceException.class, () -> service.deleteTopic("__consumer_offsets"));
        verify(admin, never()).deleteTopics(anyCollection());
    }

    @Test
    void deleteTopic_siNoExiste_propagaLaExcepcionDeKafka() {
        KafkaFutureImpl<Void> fallida = new KafkaFutureImpl<>();
        fallida.completeExceptionally(new UnknownTopicOrPartitionException("no existe"));
        DeleteTopicsResult resultado = mock(DeleteTopicsResult.class);
        when(resultado.all()).thenReturn(fallida);
        when(admin.deleteTopics(anyCollection())).thenReturn(resultado);

        assertThrows(UnknownTopicOrPartitionException.class, () -> service.deleteTopic("nada"));
    }

    // ------------------------------------------------------------- consultas

    @Test
    void listTopics_devuelveParticionesYReplicasOrdenadosPorNombre() {
        ListTopicsResult nombres = mock(ListTopicsResult.class);
        when(nombres.names()).thenReturn(KafkaFuture.<Set<String>>completedFuture(Set.of("ventas-alertas", "ventas-registradas")));
        when(admin.listTopics()).thenReturn(nombres);

        DescribeTopicsResult descripcion = mock(DescribeTopicsResult.class);
        when(descripcion.allTopicNames()).thenReturn(KafkaFuture.<Map<String, TopicDescription>>completedFuture(Map.of(
                "ventas-registradas", topicoConTresParticiones("ventas-registradas"),
                "ventas-alertas", topicoConTresParticiones("ventas-alertas"))));
        when(admin.describeTopics(anyCollection())).thenReturn(descripcion);

        List<TopicSummary> topicos = service.listTopics();

        assertEquals(2, topicos.size());
        assertEquals("ventas-alertas", topicos.get(0).name());
        assertEquals(3, topicos.get(0).partitions());
        assertEquals(3, topicos.get(0).replicationFactor());
    }

    @Test
    void describeCluster_devuelveLosBrokersOrdenados() {
        DescribeClusterResult resultado = mock(DescribeClusterResult.class);
        when(resultado.clusterId()).thenReturn(KafkaFuture.<String>completedFuture("cluster-1"));
        when(resultado.controller()).thenReturn(KafkaFuture.<Node>completedFuture(new Node(2, "kafka-2", 29092)));
        when(resultado.nodes()).thenReturn(KafkaFuture.<Collection<Node>>completedFuture(
                List.of(new Node(3, "kafka-3", 29092), new Node(1, "kafka-1", 29092), new Node(2, "kafka-2", 29092))));
        when(admin.describeCluster()).thenReturn(resultado);

        ClusterInfo info = service.describeCluster();

        assertEquals("cluster-1", info.clusterId());
        assertEquals(2, info.controllerId());
        assertEquals(List.of(1, 2, 3), info.brokers().stream().map(ClusterInfo.BrokerInfo::id).toList());
    }

    @Test
    void describeConsumerGroup_calculaElLagPorParticionYElTotal() {
        ConsumerGroupDescription grupo = mock(ConsumerGroupDescription.class);
        when(grupo.members()).thenReturn(List.of());
        DescribeConsumerGroupsResult descripcion = mock(DescribeConsumerGroupsResult.class);
        when(descripcion.all()).thenReturn(
                KafkaFuture.<Map<String, ConsumerGroupDescription>>completedFuture(Map.of("analitica-group", grupo)));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(descripcion);

        TopicPartition p0 = new TopicPartition("ventas-alertas", 0);
        TopicPartition p1 = new TopicPartition("ventas-alertas", 1);
        ListConsumerGroupOffsetsResult confirmados = mock(ListConsumerGroupOffsetsResult.class);
        when(confirmados.partitionsToOffsetAndMetadata()).thenReturn(
                KafkaFuture.<Map<TopicPartition, OffsetAndMetadata>>completedFuture(
                        Map.of(p0, new OffsetAndMetadata(10), p1, new OffsetAndMetadata(30))));
        when(admin.listConsumerGroupOffsets(anyString())).thenReturn(confirmados);

        ListOffsetsResult finales = mock(ListOffsetsResult.class);
        when(finales.all()).thenReturn(KafkaFuture.<Map<TopicPartition, ListOffsetsResultInfo>>completedFuture(Map.of(
                p0, new ListOffsetsResultInfo(25, 0L, Optional.empty()),
                p1, new ListOffsetsResultInfo(30, 0L, Optional.empty()))));
        when(admin.listOffsets(anyMap())).thenReturn(finales);

        ConsumerGroupDetail detalle = service.describeConsumerGroup("analitica-group");

        assertEquals(2, detalle.lag().size());
        assertEquals(15, detalle.lag().get(0).lag());
        assertEquals(0, detalle.lag().get(1).lag());
        assertEquals(15, detalle.totalLag());
    }

    @Test
    void describeConsumerGroup_siNoExiste_lanzaNotFound() {
        DescribeConsumerGroupsResult descripcion = mock(DescribeConsumerGroupsResult.class);
        when(descripcion.all()).thenReturn(
                KafkaFuture.<Map<String, ConsumerGroupDescription>>completedFuture(Map.of()));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(descripcion);

        assertThrows(ResourceNotFoundException.class, () -> service.describeConsumerGroup("fantasma"));
    }

    // --------------------------------------------------------------- helpers

    private void clusterConBrokers(int cantidad) {
        List<Node> nodos = new ArrayList<>();
        for (int i = 1; i <= cantidad; i++) {
            nodos.add(new Node(i, "kafka-" + i, 29092));
        }
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(cluster.nodes()).thenReturn(KafkaFuture.<Collection<Node>>completedFuture(nodos));
        when(admin.describeCluster()).thenReturn(cluster);
    }

    private CreateTopicsResult creacionExitosa() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.<Void>completedFuture(null));
        return resultado;
    }

    private NewTopic topicEnviado() {
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        return captor.getValue().iterator().next();
    }

    private TopicDescription topicoConTresParticiones(String nombre) {
        List<Node> replicas = List.of(new Node(1, "kafka-1", 29092), new Node(2, "kafka-2", 29092),
                new Node(3, "kafka-3", 29092));
        List<TopicPartitionInfo> particiones = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            particiones.add(new TopicPartitionInfo(i, replicas.get(i), replicas, replicas));
        }
        return new TopicDescription(nombre, false, particiones);
    }
}
