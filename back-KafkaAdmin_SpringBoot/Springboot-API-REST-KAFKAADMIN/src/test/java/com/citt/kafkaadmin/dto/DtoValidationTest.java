package com.citt.kafkaadmin.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DtoValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void topico_conNombreVacio_esInvalido() {
        assertFalse(validator.validate(new TopicCreateRequest("", 3, 3, null)).isEmpty());
        assertFalse(validator.validate(new TopicCreateRequest("   ", 3, 3, null)).isEmpty());
        assertFalse(validator.validate(new TopicCreateRequest(null, 3, 3, null)).isEmpty());
    }

    @Test
    void topico_conCaracteresRaros_esInvalido() {
        assertFalse(validator.validate(new TopicCreateRequest("ventas registradas/../x", 3, 3, null)).isEmpty());
    }

    @Test
    void topico_conParticionesOReplicasFueraDeRango_esInvalido() {
        assertFalse(validator.validate(new TopicCreateRequest("t1", 0, 3, null)).isEmpty());
        assertFalse(validator.validate(new TopicCreateRequest("t1", 3, 0, null)).isEmpty());
        assertFalse(validator.validate(new TopicCreateRequest("t1", 5000, 3, null)).isEmpty());
        assertFalse(validator.validate(new TopicCreateRequest("t1", 3, 11, null)).isEmpty());
    }

    @Test
    void topico_correcto_esValido() {
        assertTrue(validator.validate(new TopicCreateRequest("ventas-registradas", 3, 3,
                Map.of("retention.ms", "604800000", "cleanup.policy", "delete"))).isEmpty());
    }

    @Test
    void topico_sinParticionesNiReplicas_usaLosValoresDelProyecto() {
        TopicCreateRequest request = new TopicCreateRequest("t1", null, null, null);

        assertTrue(validator.validate(request).isEmpty());
        assertEquals(3, request.partitionsOrDefault());
        assertEquals((short) 3, request.replicationFactorOrDefault());
        assertTrue(request.configsOrEmpty().isEmpty());
    }

    @Test
    void particiones_sinValor_sonInvalidas() {
        assertFalse(validator.validate(new PartitionsUpdateRequest(null)).isEmpty());
        assertFalse(validator.validate(new PartitionsUpdateRequest(0)).isEmpty());
        assertTrue(validator.validate(new PartitionsUpdateRequest(6)).isEmpty());
    }

    @Test
    void cambioDeConfiguracion_sinListas_devuelveColeccionesVacias() {
        TopicConfigUpdateRequest request = new TopicConfigUpdateRequest(null, null);

        assertTrue(request.configsOrEmpty().isEmpty());
        assertTrue(request.removeOrEmpty().isEmpty());
    }
}
