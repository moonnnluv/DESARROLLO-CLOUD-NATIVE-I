package com.citt.streaming.rules;

import com.citt.streaming.config.PipelineProperties;
import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.Severidad;
import com.citt.streaming.model.VentaEnriquecida;
import org.springframework.stereotype.Component;

import java.util.Optional;

/** Marca como ALTA las ventas cuyo monto supera estrictamente el umbral configurado. */
@Component
public class MontoSospechosoRegla implements ReglaDeteccion {

    private final long umbralClp;

    public MontoSospechosoRegla(PipelineProperties props) {
        this.umbralClp = props.reglas().montoSospechosoClp();
    }

    @Override
    public Optional<AlertaVenta> evaluar(VentaEnriquecida enriquecida) {
        long monto = enriquecida.venta().valorCompra();
        if (monto <= umbralClp) {
            return Optional.empty();
        }
        String motivo = "Monto de " + monto + " CLP supera el umbral de " + umbralClp + " CLP";
        return Optional.of(AlertaVenta.de(enriquecida, motivo, Severidad.ALTA));
    }
}
