package com.citt.kafkaadmin.controller;

import com.citt.kafkaadmin.dto.ApiMessage;
import com.citt.kafkaadmin.dto.ClusterInfo;
import com.citt.kafkaadmin.dto.ConsumerGroupDetail;
import com.citt.kafkaadmin.dto.ConsumerGroupSummary;
import com.citt.kafkaadmin.dto.KafkaNames;
import com.citt.kafkaadmin.dto.PartitionsUpdateRequest;
import com.citt.kafkaadmin.dto.TopicConfigUpdateRequest;
import com.citt.kafkaadmin.dto.TopicCreateRequest;
import com.citt.kafkaadmin.dto.TopicDetail;
import com.citt.kafkaadmin.dto.TopicSummary;
import com.citt.kafkaadmin.service.KafkaAdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * API REST de administracion de Kafka. Recursos en plural y verbos HTTP con su significado estandar:
 * GET consulta, POST crea (201), PATCH modifica parte de un recurso, PUT reemplaza un valor, DELETE elimina (204).
 * El controller solo traduce HTTP a llamadas del servicio, sin logica de negocio.
 */
@RestController
@RequestMapping("/api/admin")
@Tag(name = "Kafka Admin", description = "Topicos, configuraciones, cluster y consumer groups")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    // -------------------------------------------------------------- topicos

    @Operation(summary = "Lista los topicos (sin los internos de Kafka)")
    @GetMapping("/topics")
    public List<TopicSummary> listTopics() {
        return service.listTopics();
    }

    @Operation(summary = "Detalle de un topico: particiones, lider, replicas, ISR y configuraciones")
    @GetMapping("/topics/{name}")
    public TopicDetail describeTopic(
            @PathVariable @Pattern(regexp = KafkaNames.TOPIC_PATTERN, message = KafkaNames.TOPIC_PATTERN_MESSAGE) String name) {
        return service.describeTopic(name);
    }

    @Operation(summary = "Crea un topico (por defecto 3 particiones y 3 replicas)")
    @PostMapping("/topics")
    public ResponseEntity<ApiMessage> createTopic(@Valid @RequestBody TopicCreateRequest request) {
        service.createTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiMessage.of(HttpStatus.CREATED.value(), "Topico '" + request.name() + "' creado"));
    }

    @Operation(summary = "Modifica la configuracion de un topico (retention.ms, cleanup.policy, etc.)")
    @PatchMapping("/topics/{name}/config")
    public ResponseEntity<ApiMessage> updateConfig(
            @PathVariable @Pattern(regexp = KafkaNames.TOPIC_PATTERN, message = KafkaNames.TOPIC_PATTERN_MESSAGE) String name,
            @RequestBody TopicConfigUpdateRequest request) {
        service.updateConfigs(name, request);
        return ok("Configuracion del topico '" + name + "' actualizada");
    }

    @Operation(summary = "Aumenta el numero de particiones de un topico (Kafka no permite reducirlo)")
    @PutMapping("/topics/{name}/partitions")
    public ResponseEntity<ApiMessage> increasePartitions(
            @PathVariable @Pattern(regexp = KafkaNames.TOPIC_PATTERN, message = KafkaNames.TOPIC_PATTERN_MESSAGE) String name,
            @Valid @RequestBody PartitionsUpdateRequest request) {
        service.increasePartitions(name, request);
        return ok("El topico '" + name + "' ahora tiene " + request.partitions() + " particiones");
    }

    @Operation(summary = "Elimina un topico y sus mensajes")
    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Void> deleteTopic(
            @PathVariable @Pattern(regexp = KafkaNames.TOPIC_PATTERN, message = KafkaNames.TOPIC_PATTERN_MESSAGE) String name) {
        service.deleteTopic(name);
        return ResponseEntity.noContent().build();
    }

    // -------------------------------------------------------------- cluster

    @Operation(summary = "Brokers del cluster y broker controlador")
    @GetMapping("/cluster")
    public ClusterInfo describeCluster() {
        return service.describeCluster();
    }

    // -------------------------------------------------------- consumer groups

    @Operation(summary = "Lista los consumer groups con su estado y cantidad de miembros")
    @GetMapping("/consumer-groups")
    public List<ConsumerGroupSummary> listConsumerGroups() {
        return service.listConsumerGroups();
    }

    @Operation(summary = "Detalle de un consumer group: miembros, particiones asignadas y lag")
    @GetMapping("/consumer-groups/{groupId}")
    public ConsumerGroupDetail describeConsumerGroup(
            @PathVariable @Pattern(regexp = KafkaNames.GROUP_PATTERN, message = KafkaNames.GROUP_PATTERN_MESSAGE) String groupId) {
        return service.describeConsumerGroup(groupId);
    }

    private ResponseEntity<ApiMessage> ok(String message) {
        return ResponseEntity.ok(ApiMessage.of(HttpStatus.OK.value(), message));
    }
}
