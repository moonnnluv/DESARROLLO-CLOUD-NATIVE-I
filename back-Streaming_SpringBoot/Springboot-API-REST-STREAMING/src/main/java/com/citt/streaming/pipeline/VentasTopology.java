package com.citt.streaming.pipeline;

import com.citt.streaming.config.PipelineProperties;
import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.VentaEnriquecida;
import com.citt.streaming.model.VentaEvento;
import com.citt.streaming.rules.ReglaDeteccion;
import com.citt.streaming.service.ConversionMonedaService;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Topologia de Kafka Streams. Patron Pipes and Filters: cada etapa recibe un flujo,
 * lo transforma y lo entrega a la siguiente.
 *
 * <pre>
 *  ventas-registradas --> [filtrar] --> [enriquecer USD] --+--> ventas-enriquecidas
 *                             |                            |
 *                             |                            +--> [reglas] --> ventas-alertas
 *                             |
 *                             +--> [agrupar por dia y sumar] --> ventas-totales-dia
 * </pre>
 */
@Component
public class VentasTopology {

    private static final Logger log = LoggerFactory.getLogger(VentasTopology.class);
    private static final String STORE_TOTALES = "totales-por-dia";

    private final PipelineProperties props;
    private final ConversionMonedaService conversion;
    private final List<ReglaDeteccion> reglas; // Spring inyecta todas las implementaciones

    public VentasTopology(PipelineProperties props,
                          ConversionMonedaService conversion,
                          List<ReglaDeteccion> reglas) {
        this.props = props;
        this.conversion = conversion;
        this.reglas = reglas;
    }

    /** Spring llama a este metodo con el StreamsBuilder que crea @EnableKafkaStreams. */
    @Autowired
    public void construir(StreamsBuilder builder) {
        Serde<VentaEvento> ventaSerde = JsonSerdes.de(VentaEvento.class);
        Serde<VentaEnriquecida> enriquecidaSerde = JsonSerdes.de(VentaEnriquecida.class);
        Serde<AlertaVenta> alertaSerde = JsonSerdes.de(AlertaVenta.class);

        // 1. LEER y FILTRAR
        KStream<String, VentaEvento> validas = builder
                .stream(props.topics().entrada(), Consumed.with(Serdes.String(), ventaSerde))
                .filter((clave, venta) -> esVentaValida(venta))
                .peek((clave, venta) -> log.info("Venta procesada: id={} fecha={} valor={} CLP",
                        venta.idVenta(), venta.fechaCompra(), venta.valorCompra()));

        // 2. ENRIQUECER (monto en USD simulado)
        KStream<String, VentaEnriquecida> enriquecidas = validas.mapValues(this::enriquecer);
        enriquecidas.to(props.topics().enriquecidas(), Produced.with(Serdes.String(), enriquecidaSerde));

        // 3. DETECTAR (cada venta puede generar 0, 1 o varias alertas)
        enriquecidas
                .flatMapValues(this::detectarAlertas)
                .peek((clave, alerta) -> log.warn("ALERTA {}: {}", alerta.severidad(), alerta.motivo()))
                .to(props.topics().alertas(), Produced.with(Serdes.String(), alertaSerde));

        // 4. AGREGAR: total vendido por dia (estado local + topic de cambios)
        validas
                .groupBy((clave, venta) -> venta.fechaCompra(), Grouped.with(Serdes.String(), ventaSerde))
                .aggregate(
                        () -> 0L,
                        (fecha, venta, acumulado) -> acumulado + venta.valorCompra(),
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_TOTALES)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()))
                .toStream()
                .to(props.topics().totales(), Produced.with(Serdes.String(), Serdes.Long()));
    }

    private boolean esVentaValida(VentaEvento venta) {
        return venta != null
                && venta.fechaCompra() != null
                && !venta.fechaCompra().isBlank()
                && venta.valorCompra() != null
                && venta.valorCompra() >= props.reglas().montoMinimoClp();
    }

    private VentaEnriquecida enriquecer(VentaEvento venta) {
        return new VentaEnriquecida(venta, conversion.clpAUsd(venta.valorCompra()));
    }

    private List<AlertaVenta> detectarAlertas(VentaEnriquecida enriquecida) {
        return reglas.stream()
                .map(regla -> regla.evaluar(enriquecida))
                .flatMap(Optional::stream)
                .toList();
    }
}
