package com.citt.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import org.springframework.context.annotation.Configuration;

/**
 * Documentacion OpenAPI. Declara el esquema de seguridad "bearerAuth" (JWT de Microsoft Entra ID)
 * para que Swagger UI muestre el boton Authorize y envie el header Authorization: Bearer <token>.
 * Es solo documentacion: la validacion real del JWT la hace SecurityConfig.
 */
@Configuration
@SecurityScheme(
        name = "bearerAuth",
        type = SecuritySchemeType.HTTP,
        scheme = "bearer",
        bearerFormat = "JWT"
)
@OpenAPIDefinition(
        info = @Info(
                title = "API REST Ventas",
                version = "1.0",
                description = "API REST Demo para gestionar ventas de productos. Lanzamiento CITT Duoc UC Viña del Mar 2025"
        )
)
public class OpenApiConfing {
}
