package com.citt.streaming.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Parametros del pipeline leidos desde application.yml (bloque "pipeline").
 * Evita "numeros magicos" en el codigo: cambiar un umbral no requiere recompilar.
 */
@ConfigurationProperties(prefix = "pipeline")
public record PipelineProperties(Topics topics, Reglas reglas, Conversion conversion) {

    public record Topics(String entrada, String enriquecidas, String alertas, String totales) {
    }

    public record Reglas(long montoMinimoClp, long montoSospechosoClp) {
    }

    public record Conversion(double clpPorUsd) {
    }
}
