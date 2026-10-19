package com.citt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Productor simple. RabbitTemplate es la clase de Spring AMQP para enviar mensajes.
 * Con exchange "" (por defecto) la routing key es el nombre de la cola destino.
 */
@Component
public class Sender {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final RabbitTemplate rabbitTemplate;

    public Sender(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /** Envia un mensaje con marca de tiempo a la cola "hello". */
    public void sendMessage(String message) {
        String fullMessage = String.format("[%s] %s", LocalDateTime.now().format(TIME), message);
        try {
            rabbitTemplate.convertAndSend(RabbitMQConfig.HELLO_QUEUE, fullMessage);
            log.info("Mensaje enviado: '{}'", fullMessage);
        } catch (Exception e) {
            log.error("Error enviando mensaje", e);
        }
    }

    /** Version con exchange y routing key explicitos. */
    public void sendMessage(String exchange, String routingKey, String message) {
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, message);
            log.info("Mensaje enviado a exchange='{}', routingKey='{}': '{}'", exchange, routingKey, message);
        } catch (Exception e) {
            log.error("Error enviando mensaje", e);
        }
    }
}
