import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { PedidosService } from '../services/pedidos.service';

@Component({
  selector: 'app-pedidos',
  imports: [CommonModule],
  template: `
    <h2>Pedidos (ventas)</h2>
    <pre>{{ ventas | json }}</pre>
    <h2>Despachos</h2>
    <pre>{{ despachos | json }}</pre>
  `
})
export class PedidosComponent implements OnInit {
  private readonly pedidosService = inject(PedidosService);
  ventas: unknown;
  despachos: unknown;

  ngOnInit(): void {
    this.pedidosService.getVentas().subscribe((data) => (this.ventas = data));
    this.pedidosService.getDespachos().subscribe((data) => (this.despachos = data));
  }
}
