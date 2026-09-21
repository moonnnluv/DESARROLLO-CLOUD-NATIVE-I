import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';

// Patron Repository/Service: centraliza el acceso HTTP al backend para que
// los componentes no sepan nada de URLs ni de como se adjunta el token
// (eso lo resuelve el MsalInterceptor de forma transversal).
@Injectable({ providedIn: 'root' })
export class PedidosService {
  private readonly http = inject(HttpClient);

  getVentas() {
    return this.http.get(environment.api.ventasBaseUrl);
  }

  getDespachos() {
    return this.http.get(environment.api.despachosBaseUrl);
  }
}
