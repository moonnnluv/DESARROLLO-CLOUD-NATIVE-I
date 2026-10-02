package com.citt.messaging;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Consumidor (Receiver): escucha la cola "hello" y muestra cada mensaje recibido.
 */
@Component
public class Receiver {

    @RabbitListener(queues = "hello")
    public void receiveMessage(String message) {
        String timestamp = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
        System.out.println("[" + timestamp + "] [✓] Mensaje recibido: '" + message + "'");
    }
}
