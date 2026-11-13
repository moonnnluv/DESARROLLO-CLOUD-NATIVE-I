# Springboot-API-REST-STREAMING

Microservicio de **Kafka Streams** de la Tienda de Perritos. Implementa el pipeline de la guía 3.2.2
sobre las ventas reales: el microservicio **Ventas** publica un evento cada vez que guarda una venta
y este servicio lo procesa en tiempo real.

```
Ventas (POST /api/v1/ventas) --> ventas-registradas --> Kafka Streams --+--> ventas-enriquecidas
                                                                         +--> ventas-alertas
                                                                         +--> ventas-totales-dia
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
| Kafka UI (`kafka-lab/compose.yaml`) | 8080 |
| Ventas | 8081 |
| Despachos | 8082 |
| RabbitAdmin | 8083 |
| **Streaming (este)** | **8084** |

## Cómo ejecutarlo (PowerShell)

1. Kafka, usando el compose que ya tienes:
   ```
   cd kafka-lab
   docker compose up -d
   docker compose ps
   ```
2. Pruebas automáticas de la topología (no necesitan Docker):
   ```
   cd back-Streaming_SpringBoot\Springboot-API-REST-STREAMING
   .\mvnw.cmd test        (o: mvn test)
   ```
3. Levantar este servicio **primero** (crea los topics, porque el compose tiene auto-create desactivado):
   ```
   mvn spring-boot:run
   ```
4. Probar sin token JWT, en otra terminal:
   ```
   .\probar-pipeline.ps1
   ```
5. Ver resultados en http://localhost:8080 → Topics → pestaña Messages:
   `ventas-enriquecidas`, `ventas-alertas`, `ventas-totales-dia`.

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
  `docker compose down -v` y `docker compose up -d` en `kafka-lab`.
- La tasa CLP a USD es **simulada**.
- `statestore.cache.max.bytes: 0` y `commit.interval.ms: 1000` son solo para ver los totales al instante.
- El endpoint `/api/simulador/ventas` es solo para laboratorio: no tiene seguridad.
