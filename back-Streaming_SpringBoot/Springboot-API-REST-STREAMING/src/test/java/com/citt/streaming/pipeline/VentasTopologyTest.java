package com.citt.streaming.pipeline;

import com.citt.streaming.config.PipelineProperties;
import com.citt.streaming.model.AlertaVenta;
import com.citt.streaming.model.Severidad;
import com.citt.streaming.model.VentaEnriquecida;
import com.citt.streaming.model.VentaEvento;
import com.citt.streaming.rules.FechaFuturaRegla;
import com.citt.streaming.rules.MontoSospechosoRegla;
import com.citt.streaming.service.ConversionMonedaService;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prueba la topologia completa SIN Kafka ni Docker usando TopologyTestDriver.
 * Patron de prueba: Arrange (armar), Act (enviar evento), Assert (leer salida).
 */
class VentasTopologyTest {

    private static final String HOY = "2026-11-12";

    private static final PipelineProperties PROPS = new PipelineProperties(
            new PipelineProperties.Topics("ventas-registradas", "ventas-enriquecidas",
                    "ventas-alertas", "ventas-totales-dia"),
            new PipelineProperties.Reglas(1_000, 500_000),
            new PipelineProperties.Conversion(950.0));

    // Reloj fijo: "hoy" siempre es 2026-11-12, asi la prueba no depende de la fecha real.
    private static final Clock RELOJ_FIJO = Clock.fixed(Instant.parse("2026-11-12T12:00:00Z"), ZoneOffset.UTC);

    private TopologyTestDriver driver;
    private TestInputTopic<String, VentaEvento> entrada;
    private TestOutputTopic<String, VentaEnriquecida> enriquecidas;
    private TestOutputTopic<String, AlertaVenta> alertas;
    private TestOutputTopic<String, Long> totales;

    @BeforeEach
    void preparar() {
        VentasTopology topologia = new VentasTopology(
                PROPS,
                new ConversionMonedaService(PROPS),
                List.of(new MontoSospechosoRegla(PROPS), new FechaFuturaRegla(RELOJ_FIJO)));

        StreamsBuilder builder = new StreamsBuilder();
        topologia.construir(builder);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streaming-ventas");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), config);

        entrada = driver.createInputTopic(PROPS.topics().entrada(),
                Serdes.String().serializer(), JsonSerdes.de(VentaEvento.class).serializer());
        enriquecidas = driver.createOutputTopic(PROPS.topics().enriquecidas(),
                Serdes.String().deserializer(), JsonSerdes.de(VentaEnriquecida.class).deserializer());
        alertas = driver.createOutputTopic(PROPS.topics().alertas(),
                Serdes.String().deserializer(), JsonSerdes.de(AlertaVenta.class).deserializer());
        totales = driver.createOutputTopic(PROPS.topics().totales(),
                Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    @AfterEach
    void cerrar() {
        driver.close();
    }

    @Test
    void ventaBajoElMinimoSeDescarta() {
        entrada.pipeInput("1", venta(1L, 500, HOY));

        assertTrue(enriquecidas.isEmpty());
        assertTrue(alertas.isEmpty());
        assertTrue(totales.isEmpty());
    }

    @Test
    void ventaSinFechaSeDescarta() {
        entrada.pipeInput("2", venta(2L, 20_000, null));

        assertTrue(enriquecidas.isEmpty());
        assertTrue(totales.isEmpty());
    }

    @Test
    void ventaNormalSeEnriqueceEnUsdYNoGeneraAlerta() {
        entrada.pipeInput("3", venta(3L, 95_000, HOY));

        VentaEnriquecida resultado = enriquecidas.readValue();
        assertEquals(100.0, resultado.montoUsd());
        assertTrue(alertas.isEmpty());
    }

    @Test
    void montoSobreElUmbralGeneraAlertaAlta() {
        entrada.pipeInput("4", venta(4L, 750_000, HOY));

        AlertaVenta alerta = alertas.readValue();
        assertEquals(Severidad.ALTA, alerta.severidad());
        assertEquals(4L, alerta.idVenta());
    }

    @Test
    void montoExactamenteEnElUmbralNoGeneraAlerta() {
        entrada.pipeInput("5", venta(5L, 500_000, HOY));

        assertTrue(alertas.isEmpty());
    }

    @Test
    void fechaFuturaGeneraAlertaMedia() {
        entrada.pipeInput("6", venta(6L, 40_000, "2030-01-01"));

        AlertaVenta alerta = alertas.readValue();
        assertEquals(Severidad.MEDIA, alerta.severidad());
    }

    @Test
    void totalPorDiaSeAcumulaYNoMezclaDias() {
        entrada.pipeInput("7", venta(7L, 10_000, HOY));
        entrada.pipeInput("8", venta(8L, 25_000, HOY));
        entrada.pipeInput("9", venta(9L, 7_000, "2026-11-11"));

        Map<String, Long> ultimoTotal = totales.readKeyValuesToMap();
        assertEquals(35_000L, ultimoTotal.get(HOY));
        assertEquals(7_000L, ultimoTotal.get("2026-11-11"));
    }

    private VentaEvento venta(Long id, long valorCompra, String fechaCompra) {
        return new VentaEvento(id, "Calle Falsa 123", valorCompra, fechaCompra, false,
                System.currentTimeMillis());
    }
}
