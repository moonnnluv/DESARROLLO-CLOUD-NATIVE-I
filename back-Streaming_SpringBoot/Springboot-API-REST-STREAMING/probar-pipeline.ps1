# Envia ventas de prueba al simulador (sin token JWT). Con la aplicacion corriendo, en PowerShell:
#   .\probar-pipeline.ps1
# Usa Invoke-RestMethod + ConvertTo-Json para evitar problemas de comillas en Windows.

$url = "http://localhost:8084/api/simulador/ventas"
$hoy = (Get-Date).ToString("yyyy-MM-dd")

function Enviar($titulo, $venta) {
    Write-Host "`n>> $titulo" -ForegroundColor Cyan
    $json = $venta | ConvertTo-Json
    Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json" -Body $json
}

Enviar "Venta NORMAL (procesada, sin alerta)" @{
    direccionCompra = "Calle Falsa 123"; valorCompra = 95000; fechaCompra = $hoy }

Enviar "Venta BAJO el minimo (descartada por el filtro)" @{
    direccionCompra = "Calle Falsa 123"; valorCompra = 500; fechaCompra = $hoy }

Enviar "Venta de MONTO ALTO (alerta ALTA)" @{
    direccionCompra = "Av. Pajaritos 456"; valorCompra = 750000; fechaCompra = $hoy }

Enviar "Venta con FECHA FUTURA (alerta MEDIA)" @{
    direccionCompra = "Los Leones 789"; valorCompra = 40000; fechaCompra = "2030-01-01" }

Enviar "Segunda venta del dia (acumula el total del dia)" @{
    direccionCompra = "Calle Falsa 123"; valorCompra = 15000; fechaCompra = $hoy }

Write-Host "`nListo. Revisa Kafka UI en http://localhost:8080 -> Topics." -ForegroundColor Green
