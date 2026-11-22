# Microservicio administrador de Kafka

Puerto 8085. Crea, lista, modifica y elimina tópicos y sus configuraciones, y permite inspeccionar el cluster y los consumer groups (con su lag).
Exige JWT de Entra ID, igual que Ventas, Despachos y RabbitAdmin. Swagger: http://localhost:8085/swagger-ui.html (público).

Se conecta al cluster de `docker/kafka` (3 brokers en `localhost:9092`, `9093` y `9094`, configurable en `application.properties`).

## Endpoints

| Método | Ruta | Respuesta |
|---|---|---|
| GET | /api/admin/topics | 200 |
| GET | /api/admin/topics/{name} | 200 / 404 |
| POST | /api/admin/topics | 201 / 400 / 409 |
| PATCH | /api/admin/topics/{name}/config | 200 / 400 / 404 |
| PUT | /api/admin/topics/{name}/partitions | 200 / 400 / 404 |
| DELETE | /api/admin/topics/{name} | 204 / 404 |
| GET | /api/admin/cluster | 200 |
| GET | /api/admin/consumer-groups | 200 |
| GET | /api/admin/consumer-groups/{groupId} | 200 / 404 |

Errores: 400 petición inválida, 404 no existe, 409 ya existe, 502 Kafka caído o sin respuesta.

## Ejemplos (reemplaza $TOKEN por un JWT válido)

```
# Crear un tópico con 3 particiones, 3 réplicas y retención de 7 días
curl -X POST localhost:8085/api/admin/topics -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"demo-topico","partitions":3,"replicationFactor":3,"configs":{"retention.ms":"604800000","min.insync.replicas":"2"}}'

# Ver detalle: líder, réplicas e ISR de cada partición
curl localhost:8085/api/admin/topics/demo-topico -H "Authorization: Bearer $TOKEN"

# Cambiar la retención a 1 día y volver min.insync.replicas a su valor por defecto
curl -X PATCH localhost:8085/api/admin/topics/demo-topico/config -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"configs":{"retention.ms":"86400000"},"remove":["min.insync.replicas"]}'

# Aumentar a 6 particiones (reducir no es posible en Kafka)
curl -X PUT localhost:8085/api/admin/topics/demo-topico/partitions -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"partitions":6}'

# Brokers del cluster y consumer groups con lag
curl localhost:8085/api/admin/cluster -H "Authorization: Bearer $TOKEN"
curl localhost:8085/api/admin/consumer-groups/alertas-group -H "Authorization: Bearer $TOKEN"

# Eliminar
curl -X DELETE localhost:8085/api/admin/topics/demo-topico -H "Authorization: Bearer $TOKEN"
```

Reglas que valida el servicio antes de llamar a Kafka: no se administran tópicos internos (`__consumer_offsets`), el factor de réplicas no puede superar la cantidad de brokers
y `min.insync.replicas` no puede superar el factor de réplicas (con `acks=all` el tópico dejaría de aceptar mensajes).

## Estructura y patrones

- `controller/`: solo traduce HTTP a llamadas del servicio.
- `service/KafkaAdminService`: toda la lógica (Facade sobre `Admin` de kafka-clients, inyectado por constructor).
- `dto/`: records con Bean Validation (DTO).
- `exceptions/RestExceptionHandler`: errores uniformes (Controller Advice), incluye la traducción de las excepciones de Kafka a códigos HTTP.
- `config/SecurityConfig`: Filter Chain con JWT. `config/KafkaAdminClientConfig`: crea el cliente `Admin` a partir de `spring.kafka.*`.

## Pruebas

```
.\mvnw.cmd test
```

Usan un mock del cliente `Admin`: no necesitan Docker ni Kafka.
