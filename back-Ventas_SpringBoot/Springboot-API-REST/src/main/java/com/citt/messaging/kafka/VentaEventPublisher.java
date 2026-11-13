package com.citt.messaging.kafka;

import com.citt.persistence.entity.Venta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publica en Kafka un evento por cada venta registrada (patron Publisher/Subscriber).
 * La publicacion es "best effort": si Kafka no responde, se registra el error pero la venta
 * ya guardada en la base de datos NO se pierde ni se revierte.
 */
@Component
public class VentaEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(VentaEventPublisher.class);

    private final KafkaTemplate<String, VentaEvento> kafka;
    private final String topic;

    public VentaEventPublisher(KafkaTemplate<String, VentaEvento> kafka,
                               @Value("${ventas.kafka.topic:ventas-registradas}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    public void publicarVentaRegistrada(Venta venta) {
        if (venta == null || venta.getIdVenta() == null) {
            return;
        }
        VentaEvento evento = VentaEvento.de(venta);
        try {
            kafka.send(topic, String.valueOf(evento.idVenta()), evento).whenComplete((resultado, error) -> {
                if (error != null) {
                    log.error("No se pudo publicar la venta {} en Kafka", evento.idVenta(), error);
                } else {
                    log.info("Venta {} publicada en {}", evento.idVenta(), topic);
                }
            });
        } catch (Exception e) {
            log.error("Kafka no disponible, la venta {} no se publico", evento.idVenta(), e);
        }
    }
}
