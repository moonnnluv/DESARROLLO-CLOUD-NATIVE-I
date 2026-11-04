package com.citt.messaging.hello;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.citt.messaging.RabbitMQConfig;
import com.rabbitmq.client.Channel;

/**
 * Consumidor (Receiver) del ejemplo basico: escucha la cola "hello" y registra cada mensaje.
 * Confirma de forma explicita: ACK si lo registro y, si algo falla, rechaza sin reencolar
 * (la cola "hello" no tiene DLX, por eso el fallo queda solo en el log).
 */
@Component
public class Receiver {

    private static final Logger log = LoggerFactory.getLogger(Receiver.class);

    @RabbitListener(queues = RabbitMQConfig.HELLO_QUEUE, ackMode = "MANUAL")
    public void receiveMessage(
            String message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        try {
            log.info("Mensaje recibido: '{}'", message);
            channel.basicAck(deliveryTag, false);
        } catch (RuntimeException e) {
            log.error("No se pudo procesar el mensaje de la cola hello: '{}'", message, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
