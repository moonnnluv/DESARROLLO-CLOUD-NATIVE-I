package com.example.loggingsystem;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String EXCHANGE_NAME = "logs_direct_exchange";
    public static final String ALL_LOGS_QUEUE = "all_logs_queue";
    public static final String ERRORS_ONLY_QUEUE = "errors_only_queue";

    @Bean
    DirectExchange logsExchange() {
        return new DirectExchange(EXCHANGE_NAME, true, false);
    }

    @Bean
    Queue allLogsQueue() {
        return QueueBuilder.durable(ALL_LOGS_QUEUE).build();
    }

    @Bean
    Queue errorsOnlyQueue() {
        return QueueBuilder.durable(ERRORS_ONLY_QUEUE).build();
    }

    @Bean
    Binding allInfo(Queue allLogsQueue, DirectExchange logsExchange) {
        return BindingBuilder.bind(allLogsQueue).to(logsExchange).with("INFO");
    }

    @Bean
    Binding allWarning(Queue allLogsQueue, DirectExchange logsExchange) {
        return BindingBuilder.bind(allLogsQueue).to(logsExchange).with("WARNING");
    }

    @Bean
    Binding allError(Queue allLogsQueue, DirectExchange logsExchange) {
        return BindingBuilder.bind(allLogsQueue).to(logsExchange).with("ERROR");
    }

    @Bean
    Binding criticalError(Queue errorsOnlyQueue, DirectExchange logsExchange) {
        return BindingBuilder.bind(errorsOnlyQueue).to(logsExchange).with("ERROR");
    }
}
