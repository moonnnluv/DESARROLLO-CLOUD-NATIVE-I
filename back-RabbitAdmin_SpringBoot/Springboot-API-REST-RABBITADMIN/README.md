# Microservicio administrador de RabbitMQ

Puerto 8083. Crea y elimina colas, exchanges y bindings. Exige JWT de Entra ID (igual que Ventas y Despachos).
Swagger: http://localhost:8083/swagger-ui.html (publico).

## Endpoints

| Metodo | Ruta | Respuesta |
|---|---|---|
| POST | /api/admin/queues | 201 / 400 / 409 |
| DELETE | /api/admin/queues/{name} | 204 / 404 |
| POST | /api/admin/exchanges | 201 / 400 |
| DELETE | /api/admin/exchanges/{name} | 204 |
| POST | /api/admin/bindings | 201 / 400 / 404 |
| DELETE | /api/admin/bindings?queue=&exchange=&routingKey= | 204 |

## Ejemplos (reemplaza $TOKEN por un JWT valido)

```
curl -X POST localhost:8083/api/admin/exchanges -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"orders.dlx","type":"FANOUT"}'

curl -X POST localhost:8083/api/admin/queues -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"orders.dlq","messageTtlMs":86400000}'

curl -X POST localhost:8083/api/admin/bindings -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"queue":"orders.dlq","exchange":"orders.dlx","routingKey":""}'

curl -X DELETE "localhost:8083/api/admin/queues/orders.dlq" -H "Authorization: Bearer $TOKEN"

# Validacion: nombre vacio responde 400
curl -X POST localhost:8083/api/admin/queues -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name":""}'
```

## Estructura y patrones

- `controller/`: solo traduce HTTP a llamadas del servicio.
- `service/RabbitAdminService`: toda la logica (Facade sobre `AmqpAdmin`, inyectado por constructor).
- `dto/`: records con Bean Validation (DTO).
- `exceptions/RestExceptionHandler`: errores uniformes (Controller Advice).
- `config/SecurityConfig`: Filter Chain con JWT.
