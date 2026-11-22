package com.citt.streaming.service;

import com.citt.streaming.exceptions.EventoInvalidoException;
import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.Severidad;
import org.springframework.stereotype.Service;

import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

/**
 * Logica de negocio de las alertas: valida cada alerta recibida y mantiene las ultimas alertas y
 * un conteo por severidad. No conoce Kafka: el consumidor le entrega objetos de dominio ya
 * deserializados, asi esta clase se prueba sin mensajeria y se puede reutilizar con otra fuente de eventos.
 * Es seguro para varios hilos porque el consumidor corre con concurrency=3.
 */
@Service
public class RegistroAlertasService {

    private static final int MAXIMO_RECIENTES = 50;

    private final Deque<AlertaVenta> recientes = new ConcurrentLinkedDeque<>();
    private final Map<Severidad, LongAdder> conteo = new EnumMap<>(Severidad.class);

    public RegistroAlertasService() {
        for (Severidad severidad : Severidad.values()) {
            conteo.put(severidad, new LongAdder());
        }
    }

    public void registrar(AlertaVenta alerta) {
        validar(alerta);
        recientes.addFirst(alerta);
        while (recientes.size() > MAXIMO_RECIENTES) {
            recientes.pollLast();
        }
        conteo.get(alerta.severidad()).increment();
    }

    /** Las alertas mas nuevas primero (maximo 50). */
    public List<AlertaVenta> recientes() {
        return List.copyOf(recientes);
    }

    public Map<Severidad, Long> conteoPorSeveridad() {
        Map<Severidad, Long> resultado = new LinkedHashMap<>();
        conteo.forEach((severidad, total) -> resultado.put(severidad, total.sum()));
        return resultado;
    }

    private void validar(AlertaVenta alerta) {
        if (alerta == null) {
            throw new EventoInvalidoException("La alerta llego vacia");
        }
        if (alerta.idVenta() == null) {
            throw new EventoInvalidoException("La alerta no trae idVenta");
        }
        if (alerta.severidad() == null) {
            throw new EventoInvalidoException("La alerta de la venta " + alerta.idVenta() + " no trae severidad");
        }
        if (alerta.motivo() == null || alerta.motivo().isBlank()) {
            throw new EventoInvalidoException("La alerta de la venta " + alerta.idVenta() + " no trae motivo");
        }
    }
}
