
package com.citt;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@CrossOrigin(origins = "http://localhost:5173")
public class OrderController {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @PostMapping("/send")
    public Map<String, String> sendOrder(
            @RequestBody Map<String, String> payload) {

        String orderId = payload.getOrDefault(
                "orderId",
                "ORD-" + System.currentTimeMillis()
        );

        String customerName = payload.getOrDefault(
                "customerName",
                "Unknown"
        );

        String message = String.format(
                "Orden ID: %s | Cliente: %s | Hora: %s",
                orderId,
                customerName,
                LocalDateTime.now().format(
                        DateTimeFormatter.ofPattern("HH:mm:ss")
                )
        );

        // Enviar el mensaje al exchange de RabbitMQ.
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDERS_EXCHANGE,
                RabbitMQConfig.ORDERS_ROUTING_KEY,
                message
        );

        Map<String, String> response = new HashMap<>();
        response.put("status", "Orden enviada");
        response.put("orderId", orderId);
        response.put("message", message);

        return response;
    }

    @GetMapping("/status")
    public Map<String, String> getStatus() {

        Map<String, String> status = new HashMap<>();
        status.put("backend", "Online");
        status.put("rabbitmq", "Conectado");
        status.put("timestamp", LocalDateTime.now().toString());

        return status;
    }
}
