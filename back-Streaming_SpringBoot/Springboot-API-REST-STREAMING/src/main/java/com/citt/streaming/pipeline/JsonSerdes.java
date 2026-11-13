package com.citt.streaming.pipeline;

import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Fabrica de Serdes JSON (Serde = serializador + deserializador de Kafka Streams).
 * Ignora los encabezados con el nombre de la clase Java para que Ventas pueda publicar
 * el mismo JSON aunque su clase este en otro paquete.
 */
public final class JsonSerdes {

    private JsonSerdes() {
    }

    public static <T> JsonSerde<T> de(Class<T> tipo) {
        return new JsonSerde<>(tipo).ignoreTypeHeaders().noTypeInfo();
    }
}
