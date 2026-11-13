package com.citt.streaming.rules;

import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.VentaEnriquecida;

import java.util.Optional;

/**
 * Patron Strategy: cada regla de sospecha es una estrategia intercambiable.
 * Para agregar una regla nueva basta crear otro @Component que implemente esta interfaz;
 * la topologia no se modifica (principio Open/Closed).
 */
public interface ReglaDeteccion {

    /** Devuelve una alerta si la venta cumple la regla, o vacio si no. */
    Optional<AlertaVenta> evaluar(VentaEnriquecida venta);
}
