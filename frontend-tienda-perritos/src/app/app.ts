import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet, RouterLink } from '@angular/router';
import { MsalService, MsalBroadcastService } from '@azure/msal-angular';
import { InteractionStatus } from '@azure/msal-browser';
import { Subject } from 'rxjs';
import { filter, takeUntil } from 'rxjs/operators';

@Component({
  selector: 'app-root',
  imports: [CommonModule, RouterOutlet, RouterLink],
  templateUrl: './app.html'
})
export class App implements OnInit, OnDestroy {
  private readonly msalService = inject(MsalService);
  private readonly msalBroadcastService = inject(MsalBroadcastService);
  private readonly destroy$ = new Subject<void>();

  isLoggedIn = false;
  userName = '';

  ngOnInit(): void {
    // Paso clave: le pedimos a MSAL que procese la respuesta de Azure AD
    // (el "code" que queda pegado en la URL tras el redirect) y la cambie
    // por los tokens reales. Sin esto, el login queda a medio camino.
    this.msalService.handleRedirectObservable().subscribe({
      next: () => this.checkAccount(),
      error: (error) => console.error('Error procesando el redirect de MSAL:', error)
    });

    // Ademas escuchamos cualquier cambio de estado de interaccion (login,
    // logout, adquisicion de token) para mantener sincronizada la UI.
    this.msalBroadcastService.inProgress$
      .pipe(
        filter((status) => status === InteractionStatus.None),
        takeUntil(this.destroy$)
      )
      .subscribe(() => this.checkAccount());
  }

  private checkAccount(): void {
    const accounts = this.msalService.instance.getAllAccounts();
    this.isLoggedIn = accounts.length > 0;
    this.userName = accounts[0]?.username ?? '';
  }

  login(): void {
    this.msalService.loginRedirect();
  }

  logout(): void {
    this.msalService.logoutRedirect();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
