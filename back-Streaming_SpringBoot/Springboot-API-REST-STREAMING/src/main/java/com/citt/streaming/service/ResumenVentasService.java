package com.citt.streaming.service;

import com.citt.streaming.exceptions.EventoInvalidoException;
import com.citt.streaming.model.VentaEnriquecida;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Acumula un resumen de las ventas enriquecidas que llegan por Kafka: cantidad y totales en CLP y USD.
 * Igual que RegistroAlertasService, no conoce Kafka y es seguro para varios hilos.
 */
@Service
public class ResumenVentasService {

    private final LongAdder ventas = new LongAdder();
    private final LongAdder totalClp = new LongAdder();
    private final DoubleAdder totalUsd = new DoubleAdder();

    public void registrar(VentaEnriquecida enriquecida) {
        validar(enriquecida);
        ventas.increment();
        totalClp.add(enriquecida.venta().valorCompra());
        totalUsd.add(enriquecida.montoUsd());
    }

    public Resumen resumen() {
        double usd = BigDecimal.valueOf(totalUsd.sum()).setScale(2, RoundingMode.HALF_UP).doubleValue();
        return new Resumen(ventas.sum(), totalClp.sum(), usd);
    }

    private void validar(VentaEnriquecida enriquecida) {
        if (enriquecida == null || enriquecida.venta() == null) {
            throw new EventoInvalidoException("La venta enriquecida llego vacia");
        }
        if (enriquecida.venta().valorCompra() == null || enriquecida.montoUsd() == null) {
            throw new EventoInvalidoException("La venta " + enriquecida.venta().idVenta()
                    + " no trae valorCompra o montoUsd");
        }
    }

    /** Totales acumulados desde que arranco el servicio. */
    public record Resumen(long ventas, long totalClp, double totalUsd) {
    }
}
