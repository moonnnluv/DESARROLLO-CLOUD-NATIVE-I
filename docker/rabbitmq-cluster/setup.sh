#!/bin/sh
# Espera a que los 2 nodos formen el cluster y carga colas, exchanges y bindings (definitions.json)
# usando la API HTTP de RabbitMQ. Se ejecuta dentro del contenedor rabbitmq-setup.
API="http://rabbit1:15672/api"
AUTH="guest:guest"

echo "Esperando que rabbit1 y rabbit2 formen el cluster..."
intentos=0
until [ "$(curl -s -u "$AUTH" "$API/nodes" | grep -o '"running":true' | wc -l)" -ge 2 ]; do
  intentos=$((intentos + 1))
  if [ "$intentos" -ge 40 ]; then
    echo "ERROR: despues de 2 minutos el cluster no tiene 2 nodos en marcha. Revisa: docker compose logs rabbit2"
    exit 1
  fi
  sleep 3
done
echo "Cluster con 2 nodos en marcha."

echo "Cargando colas, exchanges y bindings..."
curl -fsS -u "$AUTH" -H "content-type: application/json" -X POST "$API/definitions" --data-binary @/definitions.json
echo "Definiciones cargadas."
