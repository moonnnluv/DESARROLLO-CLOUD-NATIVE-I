
package com.citt.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableMethodSecurity(prePostEnabled = true)
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http)
            throws Exception {

        http
            .csrf(AbstractHttpConfigurer::disable)
            .authorizeHttpRequests(auth -> auth

                // Permitir las solicitudes previas de CORS.
                .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()

                // Permitir la documentación de Swagger.
                .requestMatchers(
                    "/swagger-ui/**",
                    "/v3/api-docs/**"
                ).permitAll()

                // Endpoints de prueba de RabbitMQ.
                // Solo para pruebas locales; no dejar públicos en producción.
                .requestMatchers(
                    HttpMethod.GET,
                    "/api/orders/status"
                ).permitAll()

                .requestMatchers(
                    HttpMethod.POST,
                    "/api/orders/send"
                ).permitAll()

                // Endpoint existente de mensajes.
                .requestMatchers("/api/messages/**").permitAll()

                // Los demás endpoints requieren un JWT válido.
                .anyRequest().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2.jwt());

        return http.build();
    }
}
