package com.citt.streaming.model;

/**
 * Evento de entrada: una venta registrada por el microservicio Ventas.
 * Los campos replican los de la entidad Venta. Es un record: inmutable y sin getters/setters manuales.
 *
 * @param idVenta         id asignado por la base de datos de Ventas
 * @param direccionCompra direccion de la compra
 * @param valorCompra     monto en pesos chilenos (CLP no usa decimales, por eso es Long)
 * @param fechaCompra     fecha de la compra en formato ISO (yyyy-MM-dd)
 * @param despachoGenerado si ya se genero el despacho
 * @param timestamp       momento en que se publico el evento (milisegundos epoch)
 */
public record VentaEvento(
        Long idVenta,
        String direccionCompra,
        Long valorCompra,
        String fechaCompra,
        Boolean despachoGenerado,
        Long timestamp) {
}
