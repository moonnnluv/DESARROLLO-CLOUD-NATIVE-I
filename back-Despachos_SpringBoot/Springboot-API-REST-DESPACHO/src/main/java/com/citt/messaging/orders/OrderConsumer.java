package com.citt.messaging.orders;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.citt.messaging.RabbitMQConfig;
import com.rabbitmq.client.Channel;

/**
 * Consumidores del dominio de ordenes: la cola principal y su Dead Letter Queue.
 *
 * Ambos listeners usan confirmacion MANUAL, asi que cada mensaje termina con una decision
 * explicita de este codigo. Para la cola principal hay tres rutas:
 *
 *   ACK          el procesamiento termino bien: basicAck, el mensaje sale de la cola.
 *   RETRY        fallo temporal y es la primera entrega: basicNack con requeue=true.
 *   DEAD_LETTER  fallo permanente, fallo inesperado o reintento agotado: basicNack con
 *                requeue=false, y RabbitMQ lo deriva al DLX configurado en la cola.
 */
@Service
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private enum Outcome { ACK, RETRY, DEAD_LETTER }

    private final OrderProcessor orderProcessor;

    public OrderConsumer(OrderProcessor orderProcessor) {
        this.orderProcessor = orderProcessor;
    }

    @RabbitListener(queues = RabbitMQConfig.ORDERS_QUEUE, ackMode = "MANUAL")
    public void processOrder(
            String message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            @Header(name = AmqpHeaders.REDELIVERED, defaultValue = "false") boolean redelivered)
            throws IOException {

        Outcome outcome = decide(message, redelivered);

        switch (outcome) {
            case ACK -> channel.basicAck(deliveryTag, false);
            case RETRY -> channel.basicNack(deliveryTag, false, true);
            case DEAD_LETTER -> channel.basicNack(deliveryTag, false, false);
        }
    }

    /**
     * Procesa la orden y traduce el resultado (o la excepcion) a una decision.
     * Separar esto del acceso al canal deja la logica de decision sin dependencias de I/O.
     */
    private Outcome decide(String message, boolean redelivered) {
        log.info("Recibida orden: {}", message);
        try {
            orderProcessor.process(message);
            log.info("Orden procesada correctamente: {}", message);
            return Outcome.ACK;

        } catch (RecoverableOrderException e) {
            if (redelivered) {
                log.error("Reintento agotado, se envia a la DLQ: {}", e.getMessage());
                return Outcome.DEAD_LETTER;
            }
            log.warn("Fallo temporal, se reintenta una vez: {}", e.getMessage());
            return Outcome.RETRY;

        } catch (UnrecoverableOrderException e) {
            log.error("Orden invalida, se envia directo a la DLQ: {}", e.getMessage());
            return Outcome.DEAD_LETTER;

        } catch (RuntimeException e) {
            // Cualquier otro error: no se reencola, para no entrar en un ciclo infinito.
            log.error("Error inesperado, se envia a la DLQ: {}", message, e);
            return Outcome.DEAD_LETTER;
        }
    }

    /**
     * Consumidor de la DLQ: registra en logs los mensajes que fallaron y los confirma.
     * Su trabajo es dejar el registro, por eso siempre hace ACK despues de loguear.
     */
    @RabbitListener(queues = RabbitMQConfig.DLQ_QUEUE, ackMode = "MANUAL")
    public void processDLQ(
            String message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.warn("MENSAJE EN CUARENTENA (DLQ): {}. Revisar los logs del procesador para analizar el fallo.", message);
        channel.basicAck(deliveryTag, false);
    }
}
