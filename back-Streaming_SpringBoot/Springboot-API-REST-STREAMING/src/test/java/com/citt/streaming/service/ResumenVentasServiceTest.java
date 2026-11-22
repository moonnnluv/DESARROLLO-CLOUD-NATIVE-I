package com.citt.streaming.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.citt.streaming.exceptions.EventoInvalidoException;
import com.citt.streaming.model.VentaEnriquecida;
import com.citt.streaming.model.VentaEvento;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ResumenVentasServiceTest {

    private ResumenVentasService service;

    @BeforeEach
    void setUp() {
        service = new ResumenVentasService();
    }

    @Test
    void registrar_acumulaCantidadYTotales() {
        service.registrar(venta(1L, 9_500L, 10.0));
        service.registrar(venta(2L, 19_000L, 20.0));

        ResumenVentasService.Resumen resumen = service.resumen();

        assertEquals(2L, resumen.ventas());
        assertEquals(28_500L, resumen.totalClp());
        assertEquals(30.0, resumen.totalUsd());
    }

    @Test
    void resumen_sinVentas_estaEnCero() {
        ResumenVentasService.Resumen resumen = service.resumen();

        assertEquals(0L, resumen.ventas());
        assertEquals(0L, resumen.totalClp());
        assertEquals(0.0, resumen.totalUsd());
    }

    @Test
    void registrar_conDatosIncompletos_lanzaEventoInvalido() {
        assertThrows(EventoInvalidoException.class, () -> service.registrar(null));
        assertThrows(EventoInvalidoException.class, () -> service.registrar(new VentaEnriquecida(null, 1.0)));
        assertThrows(EventoInvalidoException.class, () -> service.registrar(venta(1L, null, 1.0)));
        assertThrows(EventoInvalidoException.class, () -> service.registrar(venta(1L, 1_000L, null)));
    }

    private VentaEnriquecida venta(Long id, Long valor, Double usd) {
        return new VentaEnriquecida(
                new VentaEvento(id, "Av. Siempre Viva 123", valor, "2026-11-20", false, 0L), usd);
    }
}
