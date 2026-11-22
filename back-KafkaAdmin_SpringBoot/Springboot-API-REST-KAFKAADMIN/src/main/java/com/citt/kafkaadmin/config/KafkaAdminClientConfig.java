package com.citt.kafkaadmin.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Crea el cliente administrador de Kafka (Admin) como bean.
 *
 * Se reutiliza la configuracion que Spring Boot ya arma en KafkaAdmin a partir de
 * spring.kafka.* (bootstrap servers y timeouts), asi hay un solo lugar donde configurar
 * la conexion. El service depende de la interfaz Admin y no de una clase concreta,
 * lo que permite reemplazarla por un mock en las pruebas (Dependency Inversion).
 */
@Configuration
public class KafkaAdminClientConfig {

    /** destroyMethod = "close" cierra las conexiones con los brokers al detener la aplicacion. */
    @Bean(destroyMethod = "close")
    public Admin kafkaAdminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
