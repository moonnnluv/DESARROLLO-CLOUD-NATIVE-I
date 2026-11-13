package com.citt.streaming.model;

/**
 * Evento de salida publicado en el topic de alertas.
 */
public record AlertaVenta(
        Long idVenta,
        String fechaCompra,
        Long valorCompra,
        Double montoUsd,
        String motivo,
        Severidad severidad) {

    /** Factory method: centraliza como se construye una alerta a partir de una venta enriquecida. */
    public static AlertaVenta de(VentaEnriquecida enriquecida, String motivo, Severidad severidad) {
        VentaEvento venta = enriquecida.venta();
        return new AlertaVenta(
                venta.idVenta(),
                venta.fechaCompra(),
                venta.valorCompra(),
                enriquecida.montoUsd(),
                motivo,
                severidad);
    }
}
