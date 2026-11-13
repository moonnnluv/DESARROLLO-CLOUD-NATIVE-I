package com.citt.messaging.kafka;

import com.citt.persistence.entity.Venta;

/**
 * Evento que Ventas publica en Kafka cuando se registra una venta.
 * Es el contrato JSON que consume el microservicio Streaming: si cambias un campo aqui,
 * cambialo tambien en VentaEvento del proyecto Streaming.
 */
public record VentaEvento(
        Long idVenta,
        String direccionCompra,
        Long valorCompra,
        String fechaCompra,
        Boolean despachoGenerado,
        Long timestamp) {

    /** Factory method: arma el evento a partir de la entidad guardada. */
    public static VentaEvento de(Venta venta) {
        return new VentaEvento(
                venta.getIdVenta(),
                venta.getDireccionCompra(),
                (long) venta.getValorCompra(),
                venta.getFechaCompra() != null ? venta.getFechaCompra().toString() : null,
                venta.getDespachoGenerado(),
                System.currentTimeMillis());
    }
}
