import './App.css';
function App() {
  const sendLog = async (level, message) => {
    try {
      const response = await fetch('http://localhost:8080/log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level, message })
      });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(text);
      }
      alert(text);
    } catch (error) {
      console.error(error);
      alert('No se pudo enviar el log: ' + error.message);
    }
  };
  return (
    <main>
      <h1>Sistema de Logging RabbitMQ</h1>
      <div className="button-container">
        <button onClick={() => sendLog('INFO',
          'El usuario ha iniciado sesion.')}>Enviar INFO</button>
        <button onClick={() => sendLog('WARNING',
          'El uso de CPU esta al 85%.')}>Enviar WARNING</button>
        <button onClick={() => sendLog('ERROR',
          'No se pudo conectar a la base de datos.')}>Enviar ERROR</button>
      </div>
    </main>
  );
}
export default App;