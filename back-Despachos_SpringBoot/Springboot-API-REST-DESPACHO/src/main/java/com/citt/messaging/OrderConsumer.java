
package com.citt.messaging;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.rabbitmq.client.Channel;

@Service
public class OrderConsumer {

    /**
     * Consumidor principal: recibe órdenes de la cola principal.
     * Simula fallos aleatorios para demostrar el DLX.
     */
    @RabbitListener(queues = RabbitMQConfig.ORDERS_QUEUE)
    public void processOrder(
            String message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        try {
            System.out.println("[PROCESADOR] Recibida orden: " + message);

            // Simulación de fallo aleatorio
            if (Math.random() < 0.5) {
                throw new RuntimeException(
                        "Error simulado al procesar: " + message
                );
            }

            // Procesamiento exitoso
            System.out.println(
                    "[ÉXITO] Orden procesada correctamente: " + message
            );

            // Confirmar que el mensaje se procesó correctamente
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());

            try {
                // Rechazar el mensaje sin reencolarlo.
                // RabbitMQ lo deriva al DLX configurado.
                channel.basicNack(deliveryTag, false, false);

                System.out.println(
                        "[DLX] Mensaje enviado al Dead Letter Exchange"
                );

            } catch (Exception nackException) {
                nackException.printStackTrace();
            }
        }
    }

    /**
     * Consumidor de la DLQ: monitorea los mensajes que fallaron.
     */
    @RabbitListener(queues = RabbitMQConfig.DLQ_QUEUE)
    public void processDLQ(String message) {

        System.out.println(
                "[DLQ] MENSAJE EN CUARENTENA: " + message
        );

        System.out.println(
                "Revisar los logs del procesador para analizar el fallo."
        );
    }
}
