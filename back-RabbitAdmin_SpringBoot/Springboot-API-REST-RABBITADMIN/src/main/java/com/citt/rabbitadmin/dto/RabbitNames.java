package com.citt.rabbitadmin.dto;

/** Reglas de nombres compartidas por todos los DTOs (evita repetir la expresion regular). */
public final class RabbitNames {

    /** Letras, numeros, punto, guion y guion bajo. Entre 1 y 255 caracteres. */
    public static final String PATTERN = "^[A-Za-z0-9._-]{1,255}$";
    public static final String PATTERN_MESSAGE =
            "Solo se permiten letras, numeros, '.', '-' y '_' (maximo 255 caracteres)";

    private RabbitNames() {
    }
}
