package com.citt.streaming.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.ZoneId;

/**
 * Expone el reloj como bean (inyeccion de dependencias): las reglas que dependen de
 * "hoy" reciben un Clock y en las pruebas se reemplaza por uno fijo.
 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.system(ZoneId.of("America/Santiago"));
    }
}
