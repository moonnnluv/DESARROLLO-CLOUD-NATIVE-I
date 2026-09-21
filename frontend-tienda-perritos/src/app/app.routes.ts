import { Routes } from '@angular/router';
import { MsalGuard } from '@azure/msal-angular';
import { HomeComponent } from './home/home.component';
import { PedidosComponent } from './pedidos/pedidos.component';

export const routes: Routes = [
  { path: '', component: HomeComponent },
  // Patron Guard: MsalGuard bloquea la activacion de esta ruta si no hay
  // sesion activa, y dispara el login redirect automaticamente.
  { path: 'pedidos', component: PedidosComponent, canActivate: [MsalGuard] }
];
