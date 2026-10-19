package com.citt.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.rabbitmq.client.Channel;

import java.io.IOException;

/**
 * Consumidores del dominio de ordenes: la cola principal y su Dead Letter Queue.
 */
@Service
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    /**
     * Consumidor principal. Usa ackMode MANUAL solo en este listener, de modo que
     * el contenedor no confirma por su cuenta y la unica confirmacion es la nuestra
     * (basicAck / basicNack). El resto de los listeners sigue en modo AUTO.
     * Simula fallos aleatorios para demostrar el DLX.
     */
    @RabbitListener(queues = RabbitMQConfig.ORDERS_QUEUE, ackMode = "MANUAL")
    public void processOrder(
            String message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        try {
            log.info("Recibida orden: {}", message);

            // Simulacion de fallo aleatorio
            if (Math.random() < 0.5) {
                throw new IllegalStateException("Error simulado al procesar: " + message);
            }

            log.info("Orden procesada correctamente: {}", message);
            channel.basicAck(deliveryTag, false);

        } catch (IllegalStateException e) {
            // Error de procesamiento: sin reencolar, RabbitMQ lo deriva al DLX
            log.error("Fallo al procesar la orden, se envia a la DLQ: {}", e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }

    /**
     * Consumidor de la DLQ: registra en logs los mensajes que fallaron.
     */
    @RabbitListener(queues = RabbitMQConfig.DLQ_QUEUE)
    public void processDLQ(String message) {
        log.warn("MENSAJE EN CUARENTENA (DLQ): {}. Revisar los logs del procesador para analizar el fallo.", message);
    }
}
