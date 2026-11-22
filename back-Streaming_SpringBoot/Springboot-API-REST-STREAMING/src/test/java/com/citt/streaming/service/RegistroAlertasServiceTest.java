package com.citt.streaming.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.citt.streaming.exceptions.EventoInvalidoException;
import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.Severidad;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegistroAlertasServiceTest {

    private RegistroAlertasService service;

    @BeforeEach
    void setUp() {
        service = new RegistroAlertasService();
    }

    @Test
    void registrar_guardaLaAlertaYCuentaPorSeveridad() {
        service.registrar(alerta(1L, Severidad.ALTA));
        service.registrar(alerta(2L, Severidad.ALTA));
        service.registrar(alerta(3L, Severidad.MEDIA));

        assertEquals(2L, service.conteoPorSeveridad().get(Severidad.ALTA));
        assertEquals(1L, service.conteoPorSeveridad().get(Severidad.MEDIA));
        assertEquals(3, service.recientes().size());
    }

    @Test
    void recientes_devuelveLasMasNuevasPrimero() {
        service.registrar(alerta(1L, Severidad.ALTA));
        service.registrar(alerta(2L, Severidad.ALTA));

        List<AlertaVenta> recientes = service.recientes();

        assertEquals(2L, recientes.get(0).idVenta());
        assertEquals(1L, recientes.get(1).idVenta());
    }

    @Test
    void recientes_guardaComoMaximoCincuenta() {
        for (long i = 1; i <= 60; i++) {
            service.registrar(alerta(i, Severidad.MEDIA));
        }

        assertEquals(50, service.recientes().size());
        assertEquals(60L, service.recientes().get(0).idVenta());
        assertEquals(60L, service.conteoPorSeveridad().get(Severidad.MEDIA)); // el conteo no se recorta
    }

    @Test
    void registrar_conDatosIncompletos_lanzaEventoInvalido() {
        assertThrows(EventoInvalidoException.class, () -> service.registrar(null));
        assertThrows(EventoInvalidoException.class,
                () -> service.registrar(new AlertaVenta(null, "2026-11-20", 600000L, 631.57, "motivo", Severidad.ALTA)));
        assertThrows(EventoInvalidoException.class,
                () -> service.registrar(new AlertaVenta(1L, "2026-11-20", 600000L, 631.57, "motivo", null)));
        assertThrows(EventoInvalidoException.class,
                () -> service.registrar(new AlertaVenta(1L, "2026-11-20", 600000L, 631.57, "  ", Severidad.ALTA)));
    }

    private AlertaVenta alerta(long id, Severidad severidad) {
        return new AlertaVenta(id, "2026-11-20", 600000L, 631.57, "Monto sospechoso", severidad);
    }
}
