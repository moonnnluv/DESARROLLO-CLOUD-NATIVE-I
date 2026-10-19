package com.citt.rabbitadmin.service;

import com.citt.rabbitadmin.dto.BindingRequest;
import com.citt.rabbitadmin.dto.ExchangeRequest;
import com.citt.rabbitadmin.dto.QueueRequest;
import com.citt.rabbitadmin.exceptions.InvalidRabbitResourceException;
import com.citt.rabbitadmin.exceptions.ResourceAlreadyExistsException;
import com.citt.rabbitadmin.exceptions.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.stereotype.Service;

/**
 * Encapsula TODA la logica de administracion de RabbitMQ (patron Facade).
 * Los controllers solo llaman a estos metodos de alto nivel y no conocen
 * clases de Spring AMQP. La dependencia AmqpAdmin se inyecta por constructor
 * (Dependency Injection), lo que permite probar el servicio con un mock.
 */
@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

    /** Prefijo reservado por RabbitMQ para recursos internos (amq.direct, amq.fanout, etc.). */
    private static final String RESERVED_PREFIX = "amq.";

    private final AmqpAdmin amqpAdmin;

    public RabbitAdminService(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    // ---------------------------------------------------------------- colas

    public void createQueue(QueueRequest request) {
        assertNotReserved(request.name());
        if (request.deadLetterRoutingKey() != null && !request.deadLetterRoutingKey().isBlank()
                && (request.deadLetterExchange() == null || request.deadLetterExchange().isBlank())) {
            throw new InvalidRabbitResourceException(
                    "deadLetterRoutingKey requiere indicar tambien deadLetterExchange");
        }
        if (queueExists(request.name())) {
            throw new ResourceAlreadyExistsException("La cola '" + request.name() + "' ya existe");
        }

        QueueBuilder builder = request.durableOrDefault()
                ? QueueBuilder.durable(request.name())
                : QueueBuilder.nonDurable(request.name());
        if (request.messageTtlMs() != null) {
            builder.withArgument("x-message-ttl", request.messageTtlMs());
        }
        if (request.maxLength() != null) {
            builder.maxLength(request.maxLength());
        }
        if (hasText(request.deadLetterExchange())) {
            builder.deadLetterExchange(request.deadLetterExchange());
        }
        if (hasText(request.deadLetterRoutingKey())) {
            builder.deadLetterRoutingKey(request.deadLetterRoutingKey());
        }

        Queue queue = builder.build();
        amqpAdmin.declareQueue(queue);
        log.info("Cola creada: {}", queue.getName());
    }

    public void deleteQueue(String name) {
        assertNotReserved(name);
        if (!queueExists(name)) {
            throw new ResourceNotFoundException("La cola '" + name + "' no existe");
        }
        amqpAdmin.deleteQueue(name);
        log.info("Cola eliminada: {}", name);
    }

    // ------------------------------------------------------------ exchanges

    public void createExchange(ExchangeRequest request) {
        assertNotReserved(request.name());
        ExchangeBuilder builder = switch (request.type()) {
            case DIRECT -> ExchangeBuilder.directExchange(request.name());
            case FANOUT -> ExchangeBuilder.fanoutExchange(request.name());
            case TOPIC -> ExchangeBuilder.topicExchange(request.name());
            case HEADERS -> ExchangeBuilder.headersExchange(request.name());
        };
        Exchange exchange = builder.durable(request.durableOrDefault()).build();
        amqpAdmin.declareExchange(exchange);
        log.info("Exchange creado: {} ({})", exchange.getName(), request.type());
    }

    public void deleteExchange(String name) {
        assertNotReserved(name);
        // RabbitMQ no distingue "borrado" de "no existia"; la operacion es idempotente.
        amqpAdmin.deleteExchange(name);
        log.info("Exchange eliminado (o ya no existia): {}", name);
    }

    // -------------------------------------------------------------- bindings

    public void createBinding(BindingRequest request) {
        if (!queueExists(request.queue())) {
            throw new ResourceNotFoundException("La cola '" + request.queue() + "' no existe");
        }
        amqpAdmin.declareBinding(toBinding(request));
        log.info("Binding creado: {} -> {} [{}]", request.exchange(), request.queue(), request.routingKeyOrEmpty());
    }

    public void deleteBinding(BindingRequest request) {
        amqpAdmin.removeBinding(toBinding(request));
        log.info("Binding eliminado: {} -> {} [{}]", request.exchange(), request.queue(), request.routingKeyOrEmpty());
    }

    // --------------------------------------------------------------- helpers

    private Binding toBinding(BindingRequest request) {
        return new Binding(request.queue(), DestinationType.QUEUE,
                request.exchange(), request.routingKeyOrEmpty(), null);
    }

    private boolean queueExists(String name) {
        return amqpAdmin.getQueueProperties(name) != null;
    }

    private void assertNotReserved(String name) {
        if (name.startsWith(RESERVED_PREFIX)) {
            throw new InvalidRabbitResourceException(
                    "Los nombres que empiezan con '" + RESERVED_PREFIX + "' estan reservados por RabbitMQ");
        }
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
