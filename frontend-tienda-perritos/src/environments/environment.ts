export const environment = {
  production: false,
  azureAd: {
    // Directory (tenant) ID -> Azure Portal > Microsoft Entra ID > Overview
    tenantId: '296bb637-8163-48fb-9f3e-f7b1852b34e5',
    // Application (client) ID -> App registrations > TiendaDePerritos > Overview
    clientId: '197fe94c-f4bc-47b0-adc7-c88d43c9e018',
    redirectUri: 'http://localhost:4200',
    // El scope que expusiste en "Expose an API" (ej: access_as_user)
    apiScope: 'api://197fe94c-f4bc-47b0-adc7-c88d43c9e018/access_as_user'
  },
  api: {
    ventasBaseUrl: 'http://localhost:8081/api/v1/ventas',
    despachosBaseUrl: 'http://localhost:8082/api/v1/despachos'
  }
};
