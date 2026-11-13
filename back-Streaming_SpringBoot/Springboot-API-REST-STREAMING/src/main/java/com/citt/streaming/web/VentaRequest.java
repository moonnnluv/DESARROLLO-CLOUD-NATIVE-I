package com.citt.streaming.web;

import com.citt.streaming.model.VentaEvento;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.LocalDate;

/**
 * DTO de la API de simulacion. Se separa del evento de Kafka para que cambiar la API
 * no obligue a cambiar el contrato de los mensajes (y al reves).
 */
public record VentaRequest(
        Long idVenta,
        @NotBlank String direccionCompra,
        @NotNull @Positive Long valorCompra,
        LocalDate fechaCompra,
        Boolean despachoGenerado) {

    /** Mapper: convierte el DTO en evento, completando los datos opcionales que faltan. */
    public VentaEvento aEvento() {
        long ahora = System.currentTimeMillis();
        return new VentaEvento(
                idVenta != null ? idVenta : ahora,
                direccionCompra,
                valorCompra,
                (fechaCompra != null ? fechaCompra : LocalDate.now()).toString(),
                despachoGenerado != null ? despachoGenerado : Boolean.FALSE,
                ahora);
    }
}
