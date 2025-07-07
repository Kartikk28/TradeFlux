import React, { useEffect, useRef, useState } from 'react';
import './App.css';

interface Tick {
  symbol: string;
  price: number;
  timestamp: number;
}

function formatTime(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString();
}

function App() {
  const [ticks, setTicks] = useState<Tick[]>([]);
  const [health, setHealth] = useState<'ok' | 'down' | 'loading'>('loading');
  const wsRef = useRef<WebSocket | null>(null);

  // Fetch historical data
  useEffect(() => {
    fetch('/market_data')
      .then(res => res.json())
      .then(data => setTicks(data.reverse()))
      .catch(() => {});
  }, []);

  // Health check polling
  useEffect(() => {
    const check = () => {
      setHealth('loading');
      fetch('/health')
        .then(res => res.ok ? setHealth('ok') : setHealth('down'))
        .catch(() => setHealth('down'));
    };
    check();
    const interval = setInterval(check, 5000);
    return () => clearInterval(interval);
  }, []);

  // WebSocket for live updates
  useEffect(() => {
    const ws = new WebSocket(`ws://${window.location.hostname}:8000/ws/market_data`);
    wsRef.current = ws;
    ws.onmessage = (event) => {
      try {
        const tick = JSON.parse(event.data);
        setTicks(prev => [tick, ...prev].slice(0, 100));
      } catch {}
    };
    ws.onclose = () => { wsRef.current = null; };
    return () => { ws.close(); };
  }, []);

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>📈 TradeFlux Market Dashboard</h1>
        <div className={`health health-${health}`}>{health === 'ok' ? '🟢 Healthy' : health === 'loading' ? '⏳ Checking...' : '🔴 Down'}</div>
      </header>
      <main>
        <table className="tick-table">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Price ($)</th>
              <th>Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {ticks.map((tick, i) => (
              <tr key={i}>
                <td>{tick.symbol}</td>
                <td>{tick.price.toFixed(2)}</td>
                <td>{formatTime(tick.timestamp)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </main>
    </div>
  );
}

export default App;
