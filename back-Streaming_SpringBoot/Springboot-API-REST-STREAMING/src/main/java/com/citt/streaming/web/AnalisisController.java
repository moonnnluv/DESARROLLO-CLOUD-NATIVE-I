package com.citt.streaming.web;

import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.Severidad;
import com.citt.streaming.service.RegistroAlertasService;
import com.citt.streaming.service.ResumenVentasService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Consulta lo que los consumidores han recibido de Kafka. Sirve para comprobar que el sistema
 * reacciona a los eventos casi en tiempo real: se publica una venta y los totales cambian solos.
 * Como el resto de este servicio de laboratorio, no exige JWT.
 */
@RestController
@RequestMapping("/api")
public class AnalisisController {

    private final RegistroAlertasService alertas;
    private final ResumenVentasService resumen;

    public AnalisisController(RegistroAlertasService alertas, ResumenVentasService resumen) {
        this.alertas = alertas;
        this.resumen = resumen;
    }

    @GetMapping("/alertas")
    public AlertasRespuesta alertas() {
        return new AlertasRespuesta(alertas.conteoPorSeveridad(), alertas.recientes());
    }

    @GetMapping("/resumen")
    public ResumenVentasService.Resumen resumen() {
        return resumen.resumen();
    }

    /** Conteo por severidad y las ultimas alertas (las mas nuevas primero). */
    public record AlertasRespuesta(Map<Severidad, Long> conteo, List<AlertaVenta> recientes) {
    }
}
