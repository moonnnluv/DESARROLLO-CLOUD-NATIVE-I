
import { useEffect, useState } from "react";
import "./App.css";

const API_URL = "http://localhost:8081/api/orders";

function App() {
  const [orders, setOrders] = useState([]);
  const [backendStatus, setBackendStatus] = useState("Verificando...");
  const [rabbitStatus, setRabbitStatus] = useState("Verificando...");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const successfulOrders = orders.filter(
    (order) => order.status === "Enviada"
  ).length;

  const failedOrders = orders.filter(
    (order) => order.status === "Fallida"
  ).length;

  const pendingOrders = orders.filter(
    (order) => order.status === "Pendiente"
  ).length;

  async function checkStatus() {
    try {
      const response = await fetch(`${API_URL}/status`);

      if (!response.ok) {
        throw new Error("No se pudo consultar el estado");
      }

      const data = await response.json();

      setBackendStatus(data.backend || "Online");
      setRabbitStatus(data.rabbitmq || "Conectado");
    } catch {
      setBackendStatus("Sin conexión");
      setRabbitStatus("No verificado");
    }
  }

  useEffect(() => {
    checkStatus();
  }, []);

  async function sendOrder(customer) {
    if (loading) return;

    setLoading(true);
    setError("");

    const order = {
      id: Date.now(),
      customer,
      createdAt: new Date().toLocaleString("es-CL"),
      status: "Pendiente",
    };

    setOrders((previous) => [order, ...previous]);

    try {
      const response = await fetch(`${API_URL}/send`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          orderId: `ORD-${Date.now()}`,
          customerName: customer,
        }),
      });

      if (!response.ok) {
        throw new Error("El backend rechazó la orden");
      }

      setOrders((previous) =>
        previous.map((item) =>
          item.id === order.id
            ? { ...item, status: "Enviada" }
            : item
        )
      );
    } catch {
      setError(
        "No se pudo enviar la orden. Verifica que el backend esté iniciado y que CORS esté configurado."
      );

      setOrders((previous) =>
        previous.map((item) =>
          item.id === order.id
            ? { ...item, status: "Fallida" }
            : item
        )
      );
    } finally {
      setLoading(false);
    }
  }

  function clearOrders() {
    setOrders([]);
    setError("");
  }

  return (
    <div className="rabbit-page">
      <header className="header">
        <div>
          <h1>Panel de pruebas RabbitMQ</h1>
          <p>Envío de órdenes entre microservicios</p>
        </div>

        <button className="btn btn-secondary" onClick={checkStatus}>
          Actualizar estado
        </button>
      </header>

      <section className="control-panel">
        <h2>Enviar una orden de prueba</h2>
        <p>Selecciona un cliente para enviar una orden al backend.</p>

        <div className="button-group">
          <button
            className="btn btn-primary"
            disabled={loading}
            onClick={() => sendOrder("Cliente 1")}
          >
            Orden Cliente 1
          </button>

          <button
            className="btn btn-primary"
            disabled={loading}
            onClick={() => sendOrder("Cliente 2")}
          >
            Orden Cliente 2
          </button>

          <button
            className="btn btn-primary"
            disabled={loading}
            onClick={() => sendOrder("Cliente 3")}
          >
            Orden Cliente 3
          </button>
        </div>

        {loading && <p className="message">Enviando orden...</p>}
        {error && <p className="error-message">{error}</p>}
      </section>

      <section className="status-panel">
        <h2>Estado de los servicios</h2>

        <div className="status-grid">
          <div className="status-card">
            <span>Backend Ventas</span>
            <strong>{backendStatus}</strong>
          </div>

          <div className="status-card">
            <span>RabbitMQ</span>
            <strong>{rabbitStatus}</strong>
          </div>
        </div>
      </section>

      <section className="stats-panel">
        <h2>Estadísticas de las órdenes</h2>

        <div className="stats-grid">
          <div className="stat-card">
            <span>Total registradas</span>
            <strong>{orders.length}</strong>
          </div>

          <div className="stat-card">
            <span>Enviadas</span>
            <strong>{successfulOrders}</strong>
          </div>

          <div className="stat-card">
            <span>Fallidas</span>
            <strong>{failedOrders}</strong>
          </div>

          <div className="stat-card">
            <span>Pendientes</span>
            <strong>{pendingOrders}</strong>
          </div>
        </div>

        <p className="note">
          "Enviada" significa que el backend aceptó la petición HTTP.
          No confirma que el consumidor haya procesado la orden ni que
          esta haya llegado a la cola de mensajes fallidos (DLQ).
        </p>
      </section>

      <section className="orders-list">
        <div className="orders-heading">
          <h2>Historial de órdenes</h2>

          <button className="btn btn-secondary" onClick={clearOrders}>
            Limpiar historial
          </button>
        </div>

        {orders.length === 0 ? (
          <p className="empty-state">
            Todavía no hay órdenes. Envía una para comenzar la prueba.
          </p>
        ) : (
          <div className="table-container">
            <table className="orders-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Cliente</th>
                  <th>Fecha</th>
                  <th>Estado</th>
                </tr>
              </thead>

              <tbody>
                {orders.map((order) => (
                  <tr key={order.id}>
                    <td>{order.id}</td>
                    <td>{order.customer}</td>
                    <td>{order.createdAt}</td>
                    <td>
                      <span
                        className={`order-status ${order.status.toLowerCase()}`}
                      >
                        {order.status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <footer className="footer">
        <p>Prueba de integración · Spring Boot · RabbitMQ</p>
      </footer>
    </div>
  );
}

export default App;
