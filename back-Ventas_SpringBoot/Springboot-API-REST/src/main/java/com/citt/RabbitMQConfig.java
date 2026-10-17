package com.citt;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
@Configuration
public class RabbitMQConfig {

    // Exchange y cola principal de órdenes
    public static final String ORDERS_EXCHANGE = "orders.exchange";
    public static final String ORDERS_QUEUE = "orders.queue";
    public static final String ORDERS_ROUTING_KEY = "order.created";

    // Dead Letter Exchange y Dead Letter Queue
    public static final String DLX_EXCHANGE = "orders.dlx";
    public static final String DLQ_QUEUE = "orders.dlq";
    public static final String DLX_ROUTING_KEY = "order.dead";

    // Exchange principal
    @Bean
    public DirectExchange ordersExchange() {
        return new DirectExchange(ORDERS_EXCHANGE, true, false);
    }

    // Cola principal con TTL, DLX y límite de mensajes
    @Bean
    public Queue ordersQueue() {
        return QueueBuilder.durable(ORDERS_QUEUE)
                .withArgument("x-message-ttl", 30000)
                .withArgument("x-dead-letter-exchange", DLX_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DLX_ROUTING_KEY)
                .withArgument("x-max-length", 1000)
                .build();
    }

    // Conecta el exchange principal con la cola de órdenes
    @Bean
    public Binding ordersBinding() {
        return BindingBuilder
                .bind(ordersQueue())
                .to(ordersExchange())
                .with(ORDERS_ROUTING_KEY);
    }

    // Exchange de mensajes fallidos
    @Bean
    public FanoutExchange deadLetterExchange() {
        return new FanoutExchange(DLX_EXCHANGE, true, false);
    }

    // Cola donde se guardan los mensajes fallidos
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ_QUEUE)
                .withArgument("x-message-ttl", 86400000)
                .build();
    }

    // Conecta el DLX con la DLQ
    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder
                .bind(deadLetterQueue())
                .to(deadLetterExchange());
    }
}
