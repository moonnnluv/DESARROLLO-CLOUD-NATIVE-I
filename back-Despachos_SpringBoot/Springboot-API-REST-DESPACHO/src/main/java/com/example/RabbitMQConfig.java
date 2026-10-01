package com.example;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component 
public class RabbitMQConfig {

    @RabbitListener(queues = "hello")
    public void receiveMessage(String message) {
    try {
    String timestamp = LocalDateTime.now()
    .format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
    System.out.println(
            "[" + timestamp + "] [✓] Mensaje recibido: '" + message + "'"
    );

    // Aquí va tu lógica de procesamiento del mensaje
    // Si lanzas una excepción, Spring AMQP reintentará el mensaje
    // según la configuración de retry

    } catch (Exception e) {
    System.err.println(
    "[✗] Error procesando mensaje: " + e.getMessage()
    );
    e.printStackTrace();
    // Relanzar la excepción para que Spring AMQP maneje el retry
    throw new RuntimeException(e);}
    }

/**

* Método alternativo que recibe el mensaje como un objeto más
* complejo
* (Este es un ejemplo avanzado)
    */
    @RabbitListener(queues = "hello")
    public void receiveMessageAdvanced(
    String message,
    org.springframework.amqp.core.Message rawMessage,
    com.rabbitmq.client.Channel channel
    ) throws Exception {
    try {
    System.out.println("[✓] Mensaje recibido: '" + message + "'");
    System.out.println(
            " - Content-Type: "
                    + rawMessage.getMessageProperties().getContentType()
    );
    System.out.println(
            " - Timestamp: "
                    + rawMessage.getMessageProperties().getTimestamp()
    );

    // Confirmación manual del mensaje (si usas manual acks)
   //
   // channel.basicAck(
   //     rawMessage.getMessageProperties().getDeliveryTag(),
   //     false
   // );
    } catch (Exception e) {
    System.err.println("[✗] Error: " + e.getMessage());


    // Rechazar el mensaje y requearlo (si usas manual acks)
    //
    // channel.basicNack(
    //     rawMessage.getMessageProperties().getDeliveryTag(),
    //     false,
    //     true
    // );

    throw e;
    

    }
    }


}
