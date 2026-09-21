# Tienda de Perritos - Evaluación Parcial N°1 (DSY1107 - Desarrollo Cloud Native I)

**Sección:** 003D · **Grupo 7:** Antonia Arjona, Alejandra Reyes, Dante Torres

Sistema compuesto por un frontend en Angular (autenticación con Microsoft Entra ID via MSAL) y dos microservicios backend en Spring Boot (Ventas y Despachos), que validan el JWT emitido por Azure AD antes de responder cualquier petición.

## Arquitectura

```
Angular (MSAL, puerto 4200)
        |  Authorization: Bearer <JWT>
        v
Backend Ventas (Spring Boot, puerto 8081)      Backend Despachos (Spring Boot, puerto 8082)
        |                                               |
        +------------------ valida JWT contra -----------+
                                    |
                        Microsoft Entra ID (IDaaS)
                  App registrada: TiendaDePerritos
```

Ambos backends usan `spring-cloud-azure-starter-active-directory` para validar automáticamente firma, issuer y audience del token, sin necesidad de un `JwtDecoder` manual.

## Requisitos previos

- Java 17
- Maven (o usar el wrapper `./mvnw` incluido)
- Node.js 20+ y Angular CLI (`npm install -g @angular/cli`)

## Backend: Ventas (puerto 8081)

```
cd back-Ventas_SpringBoot/Springboot-API-REST
./mvnw spring-boot:run
```

## Backend: Despachos (puerto 8082)

```
cd back-Despachos_SpringBoot/Springboot-API-REST-DESPACHO
./mvnw spring-boot:run
```

> **Nota:** ambos backends usan una base de datos **H2 en memoria** (se recrea vacía en cada arranque), ya que no había acceso al RDS de AWS usado originalmente en el curso de DevOps. Esto no afecta la validación de JWT, que es lo que evalúa esta entrega. La consola de H2 queda disponible en `/h2-console` mientras la app está corriendo.

## Frontend: Angular + MSAL (puerto 4200)

```
cd frontend-tienda-perritos
npm install
ng serve
```

Luego abrir `http://localhost:4200`.

## Configuración de Microsoft Entra ID (IDaaS)

Ya está cargada en el código:

- **Tenant:** dsy1107seccion003dAle
- **App registrada:** TiendaDePerritos
- **Client ID / App ID URI:** ver `back-Ventas_SpringBoot/.../application.properties`, `back-Despachos_SpringBoot/.../application.properties` y `frontend-tienda-perritos/src/environments/environment.ts`
- **Scope expuesto:** `access_as_user`
- **Redirect URI (SPA):** `http://localhost:4200`

Client ID y App ID URI no son secretos (son identificadores públicos entregados por Azure), por eso están escritos directamente en la configuración.

## Cómo probar

1. Con los dos backends y el frontend corriendo, entra a `http://localhost:4200` y haz clic en "Iniciar sesión" (inicia el flujo OAuth2/OIDC con PKCE contra Microsoft Entra ID).
2. Tras iniciar sesión, entra a "Ver pedidos" (ruta protegida con `MsalGuard`). El `MsalInterceptor` agrega automáticamente el token JWT a las llamadas hacia Ventas y Despachos.
3. Para verificar la validación de JWT directamente: llamar `GET http://localhost:8081/api/v1/ventas` (o el equivalente de Despachos en el 8082) sin header `Authorization` debe responder **401**; con `Authorization: Bearer <token>` válido debe responder **200**.

## Estructura del repositorio

- `back-Ventas_SpringBoot/` - microservicio de Ventas
- `back-Despachos_SpringBoot/` - microservicio de Despachos
- `frontend-tienda-perritos/` - frontend Angular con MSAL (proyecto actual, usado para esta entrega)
- `front_despacho/` - frontend React reutilizado del curso de DevOps (no se usa en esta entrega, se mantiene como referencia)
