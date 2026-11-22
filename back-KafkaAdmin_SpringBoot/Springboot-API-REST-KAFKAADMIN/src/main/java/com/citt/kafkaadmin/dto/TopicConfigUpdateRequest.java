package com.citt.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/**
 * Cambios de configuracion de un topico.
 *
 * @param configs configuraciones a fijar (SET), por ejemplo {"retention.ms": "86400000"}
 * @param remove  configuraciones cuyo valor propio se elimina para volver al valor por defecto del broker (DELETE)
 */
public record TopicConfigUpdateRequest(Map<String, String> configs, List<String> remove) {

    public Map<String, String> configsOrEmpty() {
        return configs == null ? Map.of() : configs;
    }

    public List<String> removeOrEmpty() {
        return remove == null ? List.of() : remove;
    }
}
