package com.citt.streaming.service;

import com.citt.streaming.config.PipelineProperties;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Conversion CLP a USD SIMULADA con una tasa fija configurable.
 * No es un tipo de cambio real: en un sistema productivo se consultaria un proveedor de tasas.
 */
@Service
public class ConversionMonedaService {

    private final BigDecimal clpPorUsd;

    public ConversionMonedaService(PipelineProperties props) {
        this.clpPorUsd = BigDecimal.valueOf(props.conversion().clpPorUsd());
    }

    public double clpAUsd(long montoClp) {
        return BigDecimal.valueOf(montoClp)
                .divide(clpPorUsd, 2, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
