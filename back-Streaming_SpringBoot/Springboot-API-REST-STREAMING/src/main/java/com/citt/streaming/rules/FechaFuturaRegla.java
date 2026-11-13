package com.citt.streaming.rules;

import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.Severidad;
import com.citt.streaming.model.VentaEnriquecida;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/** Marca como MEDIA las ventas con fecha de compra posterior a hoy (dato inconsistente o manipulado). */
@Component
public class FechaFuturaRegla implements ReglaDeteccion {

    private final Clock clock;

    public FechaFuturaRegla(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Optional<AlertaVenta> evaluar(VentaEnriquecida enriquecida) {
        String fechaTexto = enriquecida.venta().fechaCompra();
        try {
            LocalDate fecha = LocalDate.parse(fechaTexto);
            if (!fecha.isAfter(LocalDate.now(clock))) {
                return Optional.empty();
            }
            String motivo = "Fecha de compra " + fecha + " es posterior a hoy";
            return Optional.of(AlertaVenta.de(enriquecida, motivo, Severidad.MEDIA));
        } catch (DateTimeParseException e) {
            // Una fecha con formato invalido no es "futura": la regla simplemente no aplica.
            return Optional.empty();
        }
    }
}
