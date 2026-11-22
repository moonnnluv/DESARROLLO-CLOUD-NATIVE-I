package com.citt.streaming.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.Severidad;
import com.citt.streaming.model.VentaEnriquecida;
import com.citt.streaming.model.VentaEvento;
import com.citt.streaming.service.RegistroAlertasService;
import com.citt.streaming.service.ResumenVentasService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/**
 * Los consumidores solo deben entregar el mensaje al servicio de dominio (separacion entre mensajeria y negocio).
 * Se prueban sin Kafka: se arma el ConsumerRecord a mano y se verifica la llamada al servicio.
 */
class ConsumidoresTest {

    @Test
    void alertasConsumer_entregaLaAlertaAlServicio() {
        RegistroAlertasService servicio = mock(RegistroAlertasService.class);
        AlertasConsumer consumer = new AlertasConsumer(servicio);
        AlertaVenta alerta = new AlertaVenta(7L, "2026-11-20", 600000L, 631.57, "Monto sospechoso", Severidad.ALTA);

        consumer.alRecibirAlerta(new ConsumerRecord<>("ventas-alertas", 1, 42L, "7", alerta));

        verify(servicio).registrar(alerta);
    }

    @Test
    void ventasEnriquecidasConsumer_entregaLaVentaAlServicio() {
        ResumenVentasService servicio = mock(ResumenVentasService.class);
        VentasEnriquecidasConsumer consumer = new VentasEnriquecidasConsumer(servicio);
        VentaEnriquecida venta = new VentaEnriquecida(
                new VentaEvento(7L, "Av. Siempre Viva 123", 9_500L, "2026-11-20", false, 0L), 10.0);

        consumer.alRecibirVenta(new ConsumerRecord<>("ventas-enriquecidas", 2, 5L, "7", venta));

        verify(servicio).registrar(venta);
    }
}
