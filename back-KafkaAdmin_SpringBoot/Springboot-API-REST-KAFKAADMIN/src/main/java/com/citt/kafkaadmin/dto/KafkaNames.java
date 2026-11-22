package com.citt.kafkaadmin.dto;

/** Reglas y valores por defecto compartidos por los DTOs (evita repetir expresiones regulares y numeros). */
public final class KafkaNames {

    /** Letras, numeros, punto, guion y guion bajo. Kafka admite hasta 249 caracteres en el nombre de un topico. */
    public static final String TOPIC_PATTERN = "^[A-Za-z0-9._-]{1,249}$";
    public static final String TOPIC_PATTERN_MESSAGE =
            "Solo se permiten letras, numeros, '.', '-' y '_' (maximo 249 caracteres)";

    /** Nombres de configuracion de Kafka: minusculas, numeros, punto, guion y guion bajo (ej. retention.ms). */
    public static final String CONFIG_KEY_PATTERN = "^[a-z0-9._-]{1,128}$";

    /** Los consumer groups pueden llevar mas caracteres, pero se restringen para que viajen bien en la URL. */
    public static final String GROUP_PATTERN = "^[A-Za-z0-9._-]{1,255}$";
    public static final String GROUP_PATTERN_MESSAGE =
            "Solo se permiten letras, numeros, '.', '-' y '_' (maximo 255 caracteres)";

    /** Valores del proyecto: 3 particiones y 3 replicas (1 lider + 2 seguidores en brokers distintos). */
    public static final int DEFAULT_PARTITIONS = 3;
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    public static final int MAX_PARTITIONS = 1000;
    public static final int MAX_REPLICATION_FACTOR = 10;

    private KafkaNames() {
    }
}
