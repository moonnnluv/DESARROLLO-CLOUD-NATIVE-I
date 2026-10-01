package com.example;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class Receiver {

    @RabbitListener(queues = "hello")
    public void receiveMessage(String message) {
    try {
        String timestamp = LocalDateTime.now()
        .format(DateTimeFormatter.ofPattern("HH:mm.SSS"));

        System.out.println(
                "[" + timestamp + "] [✓] Mensaje recibido: '"
                        + message + "'"
    );} catch (Exception e) {
        System.err.println(
        "[✗] Error procesando mensaje: " + e.getMessage()
        );

        e.printStackTrace(); throw new RuntimeException(e);}
    }



}
