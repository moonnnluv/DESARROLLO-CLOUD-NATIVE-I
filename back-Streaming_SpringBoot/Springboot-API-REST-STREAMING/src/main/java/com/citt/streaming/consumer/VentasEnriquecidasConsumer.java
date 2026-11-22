package com.citt.streaming.consumer;

import com.citt.streaming.model.VentaEnriquecida;
import com.citt.streaming.service.ResumenVentasService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor del topic de ventas enriquecidas (venta original mas monto en USD).
 * Igual que AlertasConsumer: solo recibe el mensaje tipado y delega en el servicio de dominio.
 *
 * Consumer group "analitica-group": es independiente de "alertas-group". Cada grupo mantiene sus
 * propios offsets, por eso ambos reciben TODOS los mensajes de su topic aunque corran en el mismo servicio.
 */
@Component
public class VentasEnriquecidasConsumer {

    private static final Logger log = LoggerFactory.getLogger(VentasEnriquecidasConsumer.class);

    private final ResumenVentasService resumen;

    public VentasEnriquecidasConsumer(ResumenVentasService resumen) {
        this.resumen = resumen;
    }

    @KafkaListener(
            topics = "${pipeline.topics.enriquecidas}",
            groupId = "analitica-group",
            properties = "spring.json.value.default.type=com.citt.streaming.model.VentaEnriquecida")
    public void alRecibirVenta(ConsumerRecord<String, VentaEnriquecida> mensaje) {
        log.info("Venta enriquecida recibida: particion={} offset={} clave={}",
                mensaje.partition(), mensaje.offset(), mensaje.key());
        resumen.registrar(mensaje.value());
    }
}
