package com.citt.streaming.web;

import com.citt.streaming.config.PipelineProperties;
import com.citt.streaming.model.VentaEvento;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Productor de prueba SOLO PARA LABORATORIO: publica una venta en Kafka sin pasar por
 * Ventas (que exige token JWT). En el flujo real, Ventas publica el evento al guardar una venta.
 */
@RestController
@RequestMapping("/api/simulador")
public class SimuladorVentasController {

    private static final Logger log = LoggerFactory.getLogger(SimuladorVentasController.class);

    private final KafkaTemplate<String, VentaEvento> kafka;
    private final String topicEntrada;

    public SimuladorVentasController(KafkaTemplate<String, VentaEvento> kafka, PipelineProperties props) {
        this.kafka = kafka;
        this.topicEntrada = props.topics().entrada();
    }

    @PostMapping("/ventas")
    public ResponseEntity<String> publicar(@Valid @RequestBody VentaRequest request) {
        VentaEvento evento = request.aEvento();

        kafka.send(topicEntrada, String.valueOf(evento.idVenta()), evento).whenComplete((resultado, error) -> {
            if (error != null) {
                log.error("No se pudo publicar la venta {}", evento.idVenta(), error);
            }
        });

        // 202 Accepted: el envio es asincrono, no confirma que el stream ya lo proceso.
        return ResponseEntity.accepted().body("Venta " + evento.idVenta() + " enviada a " + topicEntrada);
    }
}
