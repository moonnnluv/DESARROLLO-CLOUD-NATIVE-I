package com.citt.streaming.model;

/**
 * Venta original mas el monto convertido a USD (paso de enriquecimiento).
 * Se conserva la venta completa en lugar de copiar campos: no se pierde informacion.
 */
public record VentaEnriquecida(VentaEvento venta, Double montoUsd) {
}
