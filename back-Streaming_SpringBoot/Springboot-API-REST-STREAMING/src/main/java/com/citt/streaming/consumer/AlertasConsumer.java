package com.citt.streaming.consumer;

import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.service.RegistroAlertasService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor del topic de alertas. Su unica responsabilidad es la mensajeria: recibe el mensaje ya
 * convertido a AlertaVenta y se lo entrega al servicio de dominio. La logica de negocio no vive aqui.
 *
 * Consumer group "alertas-group": si se levantan varias instancias de este servicio, Kafka reparte las
 * 3 particiones entre ellas (balanceo de carga) y, si una cae, reasigna sus particiones a las demas.
 * Ademas, otro grupo (por ejemplo uno de auditoria) podria leer las mismas alertas sin afectar a este.
 */
@Component
public class AlertasConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertasConsumer.class);

    private final RegistroAlertasService registro;

    public AlertasConsumer(RegistroAlertasService registro) {
        this.registro = registro;
    }

    @KafkaListener(
            topics = "${pipeline.topics.alertas}",
            groupId = "alertas-group",
            // Tipo al que se convierte el JSON de este listener (el otro listener usa un tipo distinto)
            properties = "spring.json.value.default.type=com.citt.streaming.model.AlertaVenta")
    public void alRecibirAlerta(ConsumerRecord<String, AlertaVenta> mensaje) {
        log.info("Alerta recibida: particion={} offset={} clave={}",
                mensaje.partition(), mensaje.offset(), mensaje.key());
        registro.registrar(mensaje.value());
    }
}
