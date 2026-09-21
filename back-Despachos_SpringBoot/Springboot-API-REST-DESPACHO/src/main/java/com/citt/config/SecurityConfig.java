package com.citt.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Configuracion de seguridad como OAuth2 Resource Server, siguiendo la guia
 * oficial del curso ("Configurar Spring Security en el Backend con
 * Microsoft Entra ID y JWT").
 *
 * Patron aplicado: Filter Chain (Spring Security intercepta cada request
 * antes de llegar al controller).
 *
 * La validacion de firma, issuer y audience del JWT la hace automaticamente
 * el starter spring-cloud-azure-starter-active-directory, en base a las
 * propiedades spring.cloud.azure.active-directory.* definidas en
 * application.properties. No es necesario construir un JwtDecoder manual.
 *
 * @EnableMethodSecurity(prePostEnabled = true) habilita @PreAuthorize para
 * restringir operaciones especificas por scope, por ejemplo:
 *   @PreAuthorize("hasAuthority('SCOPE_OT.Create')")
 */
@Configuration
@EnableMethodSecurity(prePostEnabled = true)
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .authorizeHttpRequests(auth -> auth
                // Preflight CORS del navegador, siempre debe pasar sin token
                .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                // Documentacion, opcional dejarla publica
                .requestMatchers("/swagger-ui/**", "/v3/api-docs/**").permitAll()
                // Todo el resto exige un JWT valido emitido por el tenant configurado
                .anyRequest().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2.jwt());
        return http.build();
    }
}
