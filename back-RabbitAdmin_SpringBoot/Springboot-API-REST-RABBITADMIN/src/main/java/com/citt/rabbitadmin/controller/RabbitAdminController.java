package com.citt.rabbitadmin.controller;

import com.citt.rabbitadmin.dto.ApiMessage;
import com.citt.rabbitadmin.dto.BindingRequest;
import com.citt.rabbitadmin.dto.ExchangeRequest;
import com.citt.rabbitadmin.dto.QueueRequest;
import com.citt.rabbitadmin.dto.RabbitNames;
import com.citt.rabbitadmin.service.RabbitAdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * API REST de administracion de RabbitMQ. Recursos en plural y verbos HTTP con su
 * significado estandar: POST crea (201), DELETE elimina (204).
 * El controller solo traduce HTTP a llamadas del servicio, sin logica de negocio.
 */
@RestController
@RequestMapping("/api/admin")
@Tag(name = "RabbitMQ Admin", description = "Creacion y eliminacion de colas, exchanges y bindings")
public class RabbitAdminController {

    private final RabbitAdminService service;

    public RabbitAdminController(RabbitAdminService service) {
        this.service = service;
    }

    // ---------------------------------------------------------------- colas

    @Operation(summary = "Crea una cola")
    @PostMapping("/queues")
    public ResponseEntity<ApiMessage> createQueue(@Valid @RequestBody QueueRequest request) {
        service.createQueue(request);
        return created("Cola '" + request.name() + "' creada");
    }

    @Operation(summary = "Elimina una cola")
    @DeleteMapping("/queues/{name}")
    public ResponseEntity<Void> deleteQueue(
            @PathVariable @Pattern(regexp = RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE) String name) {
        service.deleteQueue(name);
        return ResponseEntity.noContent().build();
    }

    // ------------------------------------------------------------ exchanges

    @Operation(summary = "Crea un exchange")
    @PostMapping("/exchanges")
    public ResponseEntity<ApiMessage> createExchange(@Valid @RequestBody ExchangeRequest request) {
        service.createExchange(request);
        return created("Exchange '" + request.name() + "' creado");
    }

    @Operation(summary = "Elimina un exchange")
    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<Void> deleteExchange(
            @PathVariable @Pattern(regexp = RabbitNames.PATTERN, message = RabbitNames.PATTERN_MESSAGE) String name) {
        service.deleteExchange(name);
        return ResponseEntity.noContent().build();
    }

    // -------------------------------------------------------------- bindings

    @Operation(summary = "Crea un binding entre un exchange y una cola")
    @PostMapping("/bindings")
    public ResponseEntity<ApiMessage> createBinding(@Valid @RequestBody BindingRequest request) {
        service.createBinding(request);
        return created("Binding creado: " + request.exchange() + " -> " + request.queue());
    }

    @Operation(summary = "Elimina un binding (datos por query params: queue, exchange, routingKey)")
    @DeleteMapping("/bindings")
    public ResponseEntity<Void> deleteBinding(@Valid @ModelAttribute BindingRequest request) {
        service.deleteBinding(request);
        return ResponseEntity.noContent().build();
    }

    private ResponseEntity<ApiMessage> created(String message) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiMessage.of(HttpStatus.CREATED.value(), message));
    }
}
