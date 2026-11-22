# Springboot-API-REST-STREAMING

Microservicio de **Kafka Streams y consumidores Kafka** de la Tienda de Perritos. Implementa el pipeline de la guía 3.2.2
sobre las ventas reales (el microservicio **Ventas** publica un evento cada vez que guarda una venta y este servicio lo
procesa en tiempo real) y, además, consume los resultados con `@KafkaListener`, DTO, consumer groups y manejo de errores.

```
Ventas (POST /api/v1/ventas) --> ventas-registradas --> Kafka Streams --+--> ventas-enriquecidas --> VentasEnriquecidasConsumer (analitica-group)
                                                                         +--> ventas-alertas -------> AlertasConsumer (alertas-group)
                                                                         +--> ventas-totales-dia
                                         mensajes que fallan en los consumidores --> ventas-dlt
```

| Etapa de la guía 3.2.2 | Implementación aquí |
|---|---|
| Entrada | Topic `ventas-registradas` (publica Ventas) |
| Filtro | Descarta ventas bajo 1.000 CLP o sin fecha |
| Enriquecimiento a USD | `ventas-enriquecidas` con `montoUsd` (tasa simulada 950 CLP por USD) |
| Detección | `MontoSospechosoRegla` (más de 500.000 CLP, ALTA) y `FechaFuturaRegla` (fecha posterior a hoy, MEDIA) |
| Agregación | `ventas-totales-dia`: total vendido por día |
| Salida de alertas | Topic `ventas-alertas` |

Umbrales y topics se cambian en `src/main/resources/application.yml` (bloque `pipeline`).

## Puertos

| Servicio | Puerto |
|---|---|
| Kafka UI (`docker/kafka-ui`) | 8080 |
| Ventas | 8081 |
| Despachos | 8082 |
| RabbitAdmin | 8083 |
| **Streaming (este)** | **8084** |
| KafkaAdmin | 8085 |

## Clúster y tópicos

Se conecta a los tres brokers de `docker/kafka` (`localhost:9092,localhost:9093,localhost:9094`). Al iniciar crea, con
`KafkaTopicsConfig`, los tópicos del pipeline con **3 particiones, 3 réplicas y `min.insync.replicas=2`**:
`ventas-registradas`, `ventas-enriquecidas`, `ventas-alertas`, `ventas-totales-dia` (compactado) y `ventas-dlt`.
Kafka Streams guarda sus tópicos internos con `replication.factor: 3`. El productor usa `acks=all` e idempotencia.

## Consumidores

| Clase | Topic | Consumer group | Qué hace |
|---|---|---|---|
| `AlertasConsumer` | `ventas-alertas` | `alertas-group` | Guarda las últimas 50 alertas y cuenta por severidad (`RegistroAlertasService`) |
| `VentasEnriquecidasConsumer` | `ventas-enriquecidas` | `analitica-group` | Suma cantidad de ventas, total en CLP y total en USD (`ResumenVentasService`) |

- Cada consumidor recibe un `ConsumerRecord<String, DTO>` con el DTO ya convertido (`AlertaVenta`, `VentaEnriquecida`) y
  delega en un servicio de dominio. El listener no tiene lógica de negocio.
- Los dos grupos son independientes: cada uno lleva sus propios offsets, por eso ambos reciben todos los mensajes de su topic.
- Consultas: `GET /api/alertas` (conteo por severidad y alertas recientes) y `GET /api/resumen` (ventas, total CLP y total USD).

### Manejo de errores (`KafkaConsumerErrorConfig`)

1. `ErrorHandlingDeserializer` envuelve al `JsonDeserializer`: un JSON inválido no tumba al consumidor.
2. `DefaultErrorHandler` con `FixedBackOff(1000, 2)`: ante una excepción reintenta 2 veces, con 1 segundo de espera.
3. Los errores que no tiene sentido reintentar (mensaje no deserializable y `EventoInvalidoException`) van directo al tópico de fallidos.
4. `DeadLetterPublishingRecoverer` publica el mensaje original en `ventas-dlt` (Kafka elige la partición según la clave) y el consumidor sigue con el siguiente.

Para ver el flujo completo, publica un mensaje con JSON inválido en `ventas-alertas` desde Kafka UI: debería aparecer en `ventas-dlt`
sin detener al consumidor.

## Cómo ejecutarlo (PowerShell)

1. Clúster Kafka y Kafka UI (detén antes `kafka-lab/compose.yaml` si lo tenías corriendo, usa los mismos puertos):
   ```
   cd docker/kafka
   docker compose up -d
   docker compose ps

   cd ../kafka-ui
   docker compose up -d
   ```
2. Pruebas automáticas (no necesitan Docker):
   ```
   cd back-Streaming_SpringBoot\Springboot-API-REST-STREAMING
   .\mvnw.cmd test        (o: mvn test)
   ```
3. Levantar este servicio **primero** (crea los topics, porque el clúster tiene auto-create desactivado):
   ```
   .\mvnw.cmd spring-boot:run
   ```
4. Probar sin token JWT, en otra terminal:
   ```
   .\probar-pipeline.ps1
   ```
5. Ver resultados en http://localhost:8080 → Topics → pestaña Messages
   (`ventas-enriquecidas`, `ventas-alertas`, `ventas-totales-dia`) y en
   http://localhost:8084/api/alertas y http://localhost:8084/api/resumen.

Para el flujo real: con Streaming corriendo, levanta Ventas y crea una venta con `POST /api/v1/ventas`
(con tu token de Entra ID). Aparecerá la misma salida.

## Resultado esperado del script

| Venta | Log de Streaming | Topics |
|---|---|---|
| 95.000 CLP | `Venta procesada` | enriquecidas (montoUsd 100.0) y totales |
| 500 CLP | nada (filtrada) | nada |
| 750.000 CLP | `Venta procesada` + `ALERTA ALTA` | alertas con severidad ALTA |
| Fecha 2030-01-01 | `Venta procesada` + `ALERTA MEDIA` | alertas con severidad MEDIA |
| Segunda venta del día | `Venta procesada` | totales del día actualizado |

## Notas

- Si ves mensajes antiguos al reiniciar es normal: Kafka conserva los eventos. Para empezar limpio:
  `docker compose down -v` y `docker compose up -d` en `docker/kafka`.
- La configuración de un tópico solo se aplica al crearlo. Si venías de un Kafka de un solo nodo, el clúster parte vacío
  (volúmenes nuevos), así que los tópicos se crean con 3 réplicas sin más pasos.
- La tasa CLP a USD es **simulada**.
- `statestore.cache.max.bytes: 0` y `commit.interval.ms: 1000` son solo para ver los totales al instante.
- El endpoint `/api/simulador/ventas` es solo para laboratorio: no tiene seguridad. Streaming no valida JWT.
