package com.citt.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor (Receiver): escucha la cola "hello" y registra cada mensaje recibido.
 */
@Component
public class Receiver {

    private static final Logger log = LoggerFactory.getLogger(Receiver.class);

    @RabbitListener(queues = RabbitMQConfig.HELLO_QUEUE)
    public void receiveMessage(String message) {
        log.info("Mensaje recibido: '{}'", message);
    }
}
