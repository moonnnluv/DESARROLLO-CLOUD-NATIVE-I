package com.citt.streaming.config;

import com.citt.streaming.exceptions.EventoInvalidoException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Estrategia de errores de los consumidores (@KafkaListener). Spring Boot la aplica solo a todos
 * los listeners porque la expone como bean CommonErrorHandler.
 *
 * Que pasa con un mensaje defectuoso:
 *  1. JSON roto (no se puede deserializar): ErrorHandlingDeserializer atrapa el fallo y el mensaje va
 *     directo al Dead Letter Topic, sin reintentos (reintentar no lo arreglaria).
 *  2. Evento invalido (EventoInvalidoException): tampoco se reintenta, va directo al Dead Letter Topic.
 *  3. Cualquier otro error al procesar: se reintenta 2 veces con 1 segundo de espera y, si sigue fallando,
 *     va al Dead Letter Topic.
 * En todos los casos el consumidor sigue con el mensaje siguiente: un mensaje malo no detiene el flujo.
 */
@Configuration
public class KafkaConsumerErrorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerErrorConfig.class);

    private static final long ESPERA_ENTRE_REINTENTOS_MS = 1000L;
    private static final long REINTENTOS = 2L;

    @Bean
    public CommonErrorHandler kafkaErrorHandler(ProducerFactory<Object, Object> producerFactory,
                                                @Value("${pipeline.dlt-topic:ventas-dlt}") String dltTopic) {
        // Dos plantillas porque al Dead Letter Topic llegan dos tipos de valor: los bytes originales de un
        // mensaje que no se pudo deserializar (se reenvian tal cual) y objetos de dominio (se envian como JSON).
        KafkaOperations<Object, Object> plantillaBytes = new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        KafkaOperations<Object, Object> plantillaJson = new KafkaTemplate<>(producerFactory);

        // El orden importa: se usa la primera plantilla cuyo tipo coincide con el valor del mensaje.
        Map<Class<?>, KafkaOperations<?, ?>> plantillas = new LinkedHashMap<>();
        plantillas.put(byte[].class, plantillaBytes);
        plantillas.put(Object.class, plantillaJson);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(plantillas,
                (mensaje, error) -> {
                    log.error("Mensaje enviado a {}: topic={} particion={} offset={} causa={}",
                            dltTopic, mensaje.topic(), mensaje.partition(), mensaje.offset(), error.getMessage());
                    // Particion -1: el productor elige la particion del Dead Letter Topic
                    return new TopicPartition(dltTopic, -1);
                });

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(ESPERA_ENTRE_REINTENTOS_MS, REINTENTOS));
        handler.addNotRetryableExceptions(EventoInvalidoException.class);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> mensaje, Exception error, int intento) {
                log.warn("Fallo el procesamiento (intento {}): topic={} particion={} offset={} causa={}",
                        intento, mensaje.topic(), mensaje.partition(), mensaje.offset(), error.getMessage());
            }
        });
        return handler;
    }
}
