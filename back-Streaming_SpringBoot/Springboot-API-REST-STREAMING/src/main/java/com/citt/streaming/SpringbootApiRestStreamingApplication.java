package com.citt.streaming;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams          // Crea el StreamsBuilder y arranca Kafka Streams junto con Spring
@ConfigurationPropertiesScan // Registra PipelineProperties leyendo el bloque "pipeline" del yml
public class SpringbootApiRestStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootApiRestStreamingApplication.class, args);
    }
}
