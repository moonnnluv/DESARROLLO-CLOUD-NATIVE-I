package com.citt.kafkaadmin.service;

import com.citt.kafkaadmin.dto.ClusterInfo;
import com.citt.kafkaadmin.dto.ConsumerGroupDetail;
import com.citt.kafkaadmin.dto.ConsumerGroupSummary;
import com.citt.kafkaadmin.dto.KafkaNames;
import com.citt.kafkaadmin.dto.PartitionsUpdateRequest;
import com.citt.kafkaadmin.dto.TopicConfigUpdateRequest;
import com.citt.kafkaadmin.dto.TopicCreateRequest;
import com.citt.kafkaadmin.dto.TopicDetail;
import com.citt.kafkaadmin.dto.TopicSummary;
import com.citt.kafkaadmin.exceptions.InvalidKafkaResourceException;
import com.citt.kafkaadmin.exceptions.KafkaUnavailableException;
import com.citt.kafkaadmin.exceptions.ResourceNotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Encapsula TODA la logica de administracion de Kafka (patron Facade).
 * Los controllers solo llaman a estos metodos de alto nivel y no conocen la API de AdminClient.
 * El cliente Admin se inyecta por constructor (Dependency Injection), por eso las pruebas lo reemplazan por un mock.
 *
 * Las operaciones de AdminClient son asincronas y devuelven KafkaFuture. El metodo await() las convierte
 * en llamadas bloqueantes con tiempo maximo y traduce los errores de Kafka a excepciones que el
 * RestExceptionHandler transforma en respuestas HTTP.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private static final long TIMEOUT_SECONDS = 15;

    /** Los topicos que empiezan con "__" son internos de Kafka (ej. __consumer_offsets): no se tocan. */
    private static final String INTERNAL_PREFIX = "__";
    private static final String MIN_ISR = "min.insync.replicas";

    private final Admin admin;

    public KafkaAdminService(Admin admin) {
        this.admin = admin;
    }

    // --------------------------------------------------------------- topicos

    public List<TopicSummary> listTopics() {
        Set<String> names = await(admin.listTopics().names());
        if (names.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> described = await(admin.describeTopics(names).allTopicNames());
        return described.values().stream()
                .map(d -> new TopicSummary(d.name(), d.partitions().size(), replicationFactorOf(d)))
                .sorted(Comparator.comparing(TopicSummary::name))
                .toList();
    }

    public TopicDetail describeTopic(String name) {
        TopicDescription description = await(admin.describeTopics(List.of(name)).allTopicNames()).get(name);
        if (description == null) {
            throw new ResourceNotFoundException("El topico '" + name + "' no existe");
        }

        ConfigResource resource = topicResource(name);
        Config config = await(admin.describeConfigs(List.of(resource)).all()).get(resource);

        List<TopicDetail.PartitionDetail> partitions = description.partitions().stream()
                .map(this::toPartitionDetail)
                .toList();
        return new TopicDetail(name, partitions.size(), replicationFactorOf(description), partitions,
                topicLevelConfigs(config));
    }

    public void createTopic(TopicCreateRequest request) {
        String name = request.name();
        assertUsableName(name);

        int partitions = request.partitionsOrDefault();
        short replicationFactor = request.replicationFactorOrDefault();
        Map<String, String> configs = request.configsOrEmpty();
        assertConfigKeys(configs.keySet());

        int brokers = await(admin.describeCluster().nodes()).size();
        if (replicationFactor > brokers) {
            throw new InvalidKafkaResourceException("replicationFactor (" + replicationFactor
                    + ") no puede ser mayor que los brokers disponibles (" + brokers + ")");
        }
        assertMinIsrCoherent(configs, replicationFactor);

        NewTopic topic = new NewTopic(name, partitions, replicationFactor).configs(configs);
        await(admin.createTopics(List.of(topic)).all());
        log.info("Topico creado: {} ({} particiones, {} replicas, configs={})",
                name, partitions, replicationFactor, configs);
    }

    public void updateConfigs(String name, TopicConfigUpdateRequest request) {
        assertUsableName(name);

        Map<String, String> toSet = request.configsOrEmpty();
        List<String> toRemove = request.removeOrEmpty();
        if (toSet.isEmpty() && toRemove.isEmpty()) {
            throw new InvalidKafkaResourceException("Indica al menos una configuracion en 'configs' o en 'remove'");
        }
        assertConfigKeys(toSet.keySet());
        assertConfigKeys(toRemove);
        if (toRemove.stream().anyMatch(toSet::containsKey)) {
            throw new InvalidKafkaResourceException("Una misma configuracion no puede estar en 'configs' y en 'remove'");
        }
        if (toSet.containsKey(MIN_ISR)) {
            // min.insync.replicas mayor que las replicas dejaria al topico sin poder recibir mensajes con acks=all
            assertMinIsrCoherent(toSet, describeTopic(name).replicationFactor());
        }

        List<AlterConfigOp> operations = new ArrayList<>();
        toSet.forEach((key, value) ->
                operations.add(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)));
        toRemove.forEach(key ->
                operations.add(new AlterConfigOp(new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE)));

        Map<ConfigResource, Collection<AlterConfigOp>> changes = Map.of(topicResource(name), operations);
        await(admin.incrementalAlterConfigs(changes).all());
        log.info("Configuracion actualizada en {}: set={} remove={}", name, toSet, toRemove);
    }

    public void increasePartitions(String name, PartitionsUpdateRequest request) {
        assertUsableName(name);
        // Kafka rechaza el cambio (InvalidPartitionsException) si el total pedido no es mayor al actual
        await(admin.createPartitions(Map.of(name, NewPartitions.increaseTo(request.partitions()))).all());
        log.info("Particiones de {} aumentadas a {}", name, request.partitions());
    }

    public void deleteTopic(String name) {
        assertUsableName(name);
        await(admin.deleteTopics(List.of(name)).all());
        log.info("Topico eliminado: {}", name);
    }

    // --------------------------------------------------------------- cluster

    public ClusterInfo describeCluster() {
        DescribeClusterResult result = admin.describeCluster();
        String clusterId = await(result.clusterId());
        Node controller = await(result.controller());
        List<ClusterInfo.BrokerInfo> brokers = await(result.nodes()).stream()
                .map(n -> new ClusterInfo.BrokerInfo(n.id(), n.host(), n.port()))
                .sorted(Comparator.comparingInt(ClusterInfo.BrokerInfo::id))
                .toList();
        return new ClusterInfo(clusterId, controller == null ? -1 : controller.id(), brokers);
    }

    // ------------------------------------------------------- consumer groups

    public List<ConsumerGroupSummary> listConsumerGroups() {
        Collection<ConsumerGroupListing> listings = await(admin.listConsumerGroups().all());
        if (listings.isEmpty()) {
            return List.of();
        }
        List<String> ids = listings.stream().map(ConsumerGroupListing::groupId).toList();
        Map<String, ConsumerGroupDescription> described = await(admin.describeConsumerGroups(ids).all());
        return described.values().stream()
                .map(d -> new ConsumerGroupSummary(d.groupId(), String.valueOf(d.state()), d.members().size()))
                .sorted(Comparator.comparing(ConsumerGroupSummary::groupId))
                .toList();
    }

    public ConsumerGroupDetail describeConsumerGroup(String groupId) {
        ConsumerGroupDescription description =
                await(admin.describeConsumerGroups(List.of(groupId)).all()).get(groupId);
        String state = description == null ? "DEAD" : String.valueOf(description.state());
        if (description == null || "DEAD".equals(state)) {
            throw new ResourceNotFoundException("El consumer group '" + groupId + "' no existe");
        }

        List<ConsumerGroupDetail.MemberInfo> members = description.members().stream()
                .map(this::toMemberInfo)
                .toList();
        List<ConsumerGroupDetail.PartitionLag> lag = calculateLag(groupId);
        long totalLag = lag.stream().mapToLong(ConsumerGroupDetail.PartitionLag::lag).sum();
        return new ConsumerGroupDetail(groupId, state, members, lag, totalLag);
    }

    /** Lag = ultimo offset del topico menos el ultimo offset confirmado por el grupo, por cada particion. */
    private List<ConsumerGroupDetail.PartitionLag> calculateLag(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> committed =
                await(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
        if (committed.isEmpty()) {
            return List.of();
        }

        Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
        committed.keySet().forEach(partition -> latestRequest.put(partition, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResultInfo> latest = await(admin.listOffsets(latestRequest).all());

        return committed.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .map(entry -> {
                    TopicPartition partition = entry.getKey();
                    long current = entry.getValue().offset();
                    ListOffsetsResultInfo end = latest.get(partition);
                    long endOffset = end == null ? current : end.offset();
                    return new ConsumerGroupDetail.PartitionLag(partition.topic(), partition.partition(),
                            current, endOffset, Math.max(0, endOffset - current));
                })
                .sorted(Comparator.comparing(ConsumerGroupDetail.PartitionLag::topic)
                        .thenComparingInt(ConsumerGroupDetail.PartitionLag::partition))
                .toList();
    }

    // --------------------------------------------------------------- helpers

    private TopicDetail.PartitionDetail toPartitionDetail(TopicPartitionInfo info) {
        int leader = info.leader() == null ? -1 : info.leader().id();
        return new TopicDetail.PartitionDetail(info.partition(), leader,
                info.replicas().stream().map(Node::id).toList(),
                info.isr().stream().map(Node::id).toList());
    }

    private ConsumerGroupDetail.MemberInfo toMemberInfo(MemberDescription member) {
        List<String> assigned = member.assignment().topicPartitions().stream()
                .map(tp -> tp.topic() + "-" + tp.partition())
                .sorted()
                .toList();
        return new ConsumerGroupDetail.MemberInfo(member.clientId(), member.host(), assigned);
    }

    /** Todas las particiones de un topico tienen las mismas replicas; se mira la primera. */
    private int replicationFactorOf(TopicDescription description) {
        return description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size();
    }

    /** Solo las configuraciones definidas a nivel de topico (no las heredadas del broker ni las de fabrica). */
    private Map<String, String> topicLevelConfigs(Config config) {
        Map<String, String> result = new TreeMap<>();
        if (config == null) {
            return result;
        }
        for (ConfigEntry entry : config.entries()) {
            if (entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG && entry.value() != null) {
                result.put(entry.name(), entry.value());
            }
        }
        return result;
    }

    private ConfigResource topicResource(String name) {
        return new ConfigResource(ConfigResource.Type.TOPIC, name);
    }

    private void assertUsableName(String name) {
        if (name.startsWith(INTERNAL_PREFIX)) {
            throw new InvalidKafkaResourceException(
                    "Los topicos que empiezan con '" + INTERNAL_PREFIX + "' son internos de Kafka y no se administran");
        }
        if (".".equals(name) || "..".equals(name)) {
            throw new InvalidKafkaResourceException("'.' y '..' no son nombres de topico validos");
        }
    }

    private void assertConfigKeys(Collection<String> keys) {
        for (String key : keys) {
            if (key == null || !key.matches(KafkaNames.CONFIG_KEY_PATTERN)) {
                throw new InvalidKafkaResourceException("Nombre de configuracion no valido: '" + key + "'");
            }
        }
    }

    private void assertMinIsrCoherent(Map<String, String> configs, int replicationFactor) {
        String value = configs.get(MIN_ISR);
        if (value == null) {
            return;
        }
        int minIsr;
        try {
            minIsr = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new InvalidKafkaResourceException(MIN_ISR + " debe ser un numero entero");
        }
        if (minIsr < 1 || minIsr > replicationFactor) {
            throw new InvalidKafkaResourceException(MIN_ISR + " debe estar entre 1 y el factor de replicacion ("
                    + replicationFactor + ")");
        }
    }

    /** Espera el resultado de una operacion asincrona de Kafka con un tiempo maximo. */
    private <T> T await(KafkaFuture<T> future) {
        try {
            return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime; // normalmente una excepcion de Kafka que el RestExceptionHandler sabe traducir
            }
            throw new KafkaUnavailableException("Kafka rechazo la operacion: " + cause, cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException("La operacion con Kafka fue interrumpida", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new KafkaUnavailableException("Kafka no respondio en " + TIMEOUT_SECONDS + " segundos", e);
        }
    }
}
