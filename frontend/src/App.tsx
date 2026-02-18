import React, { useEffect, useMemo, useState } from "react"
import CandleChart from "./components/CandleChart"
import TradeTape from "./components/TradeTape"

type Candle = { ts: string; open: number; high: number; low: number; close: number; volume: number }
type Metrics = { ema?: number | null; vwap?: number | null }

const API: string = (import.meta as any).env?.VITE_API_URL ?? "http://localhost:8000"

export default function App() {
  const [symbols, setSymbols] = useState<string[]>([])
  const [query, setQuery] = useState("")
  const [symbol, setSymbol] = useState("ACME")
  const [windowStr, setWindowStr] = useState("15m")
  const [limit, setLimit] = useState(200)
  const [candles, setCandles] = useState<Candle[]>([])
  const [metrics, setMetrics] = useState<Metrics>({})
  const [latency, setLatency] = useState<number | null>(null)
  const [showEMA, setShowEMA] = useState(true)
  const [status, setStatus] = useState<"live" | "idle" | "error">("idle")
  const [err, setErr] = useState<string | null>(null)

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return symbols
    return symbols.filter(s => s.toLowerCase().includes(q))
  }, [symbols, query])

  async function loadSymbols() {
    const r = await fetch(`${API}/api/symbols`)
    const j = await r.json()
    const syms: string[] = j.symbols || []
    setSymbols(syms)
    if (syms.length && !syms.includes(symbol)) setSymbol(syms[0])
  }

  async function loadOHLC() {
    const url = `${API}/api/ohlc?symbol=${encodeURIComponent(symbol)}&window=${encodeURIComponent(windowStr)}&limit=${limit}`
    const r = await fetch(url)
    const j = await r.json()
    setCandles(j.candles || [])
  }

  async function loadMetrics() {
    try {
      const r = await fetch(`${API}/api/metrics?symbol=${encodeURIComponent(symbol)}`)
      const j = await r.json()
      setMetrics(j.metrics || {})
    } catch { /* non-critical */ }
  }

  async function loadHealth() {
    try {
      const r = await fetch(`${API}/api/health`)
      const j = await r.json()
      setLatency(j?.ingest_latency_ms?.p50 ?? null)
    } catch { /* non-critical */ }
  }

  useEffect(() => {
    loadSymbols().catch(() => {})
  }, [])

  useEffect(() => {
    setErr(null)
    loadOHLC().catch(e => setErr(String(e)))
    loadMetrics().catch(() => {})
  }, [symbol, windowStr, limit])

  useEffect(() => {
    setStatus("live")
    setErr(null)

    const wsUrl = API.replace(/^http/, "ws")
    const ws = new WebSocket(`${wsUrl}/ws/stream?symbol=${encodeURIComponent(symbol)}`)

    ws.onopen = () => setStatus("live")
    ws.onerror = () => setStatus("error")
    ws.onclose = () => setStatus("idle")

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data)
        if (msg?.metrics) setMetrics(msg.metrics)
        if (msg?.ingest_latency_ms != null) setLatency(msg.ingest_latency_ms)
      } catch { /* ignore */ }
      loadOHLC().catch(() => {})
    }

    const healthInterval = setInterval(loadHealth, 10_000)

    return () => {
      ws.close()
      clearInterval(healthInterval)
    }
  }, [symbol, windowStr, limit])

  return (
    <div className="appShell">
      <aside className="sidebar">
        <div className="brand" style={{ marginBottom: 14 }}>
          <div className="logoMark" />
          <div>
            <div style={{ fontSize: 16 }}>TradeFlux</div>
            <div className="mini">Streaming terminal</div>
          </div>
        </div>

        <div style={{ display: "grid", gap: 10, marginBottom: 14 }}>
          <input
            className="input"
            placeholder="Search symbols"
            value={query}
            onChange={e => setQuery(e.target.value)}
          />
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            <select className="select" value={windowStr} onChange={e => setWindowStr(e.target.value)}>
              <option value="1m">1m</option>
              <option value="5m">5m</option>
              <option value="15m">15m</option>
              <option value="1h">1h</option>
              <option value="4h">4h</option>
              <option value="1d">1d</option>
            </select>

            <select className="select" value={String(limit)} onChange={e => setLimit(Number(e.target.value))}>
              <option value="100">100</option>
              <option value="200">200</option>
              <option value="400">400</option>
              <option value="800">800</option>
            </select>
          </div>

          <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, color: "var(--muted)", cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={showEMA}
              onChange={e => setShowEMA(e.target.checked)}
              style={{ accentColor: "var(--accent)" }}
            />
            Show EMA-20
          </label>

          <button className="btn primary" onClick={() => { loadSymbols(); loadOHLC() }}>
            Refresh
          </button>
        </div>

        <div className="panel" style={{ padding: 12 }}>
          <div className="panelTitle" style={{ marginBottom: 10 }}>Symbols</div>
          <div className="list">
            {filtered.length === 0 && <div className="mini">No symbols</div>}
            {filtered.map(s => (
              <div
                key={s}
                className={`listItem ${s === symbol ? "active" : ""}`}
                onClick={() => setSymbol(s)}
              >
                <div style={{ fontWeight: 800 }}>{s}</div>
                <div className="mini">{s === symbol ? "selected" : "tap"}</div>
              </div>
            ))}
          </div>
        </div>
      </aside>

      <section className="main">
        <header className="topbar">
          <div>
            <div style={{ fontSize: 18, fontWeight: 900 }}>{symbol} candles</div>
            <div className="mini">Window {windowStr} · {candles.length} points</div>
          </div>

          <div className="controls">
            <span className={`badge ${status === "live" ? "good" : status === "error" ? "bad" : ""}`}>
              {status === "live" ? "LIVE" : status === "error" ? "ERROR" : "IDLE"}
            </span>
            {latency != null && (
              <span className="badge">p50 {latency.toFixed(1)} ms</span>
            )}
            {err && <span className="badge bad">{err}</span>}
          </div>
        </header>

        <div className="grid">
          {/* ── Candle chart ────────────────────────────────────────── */}
          <div className="panel">
            <div className="panelHeader">
              <div>
                <div className="panelTitle">{symbol} chart</div>
                <div className="panelSub">OHLCV candlesticks · EMA-20 overlay</div>
              </div>
              <div className="controls">
                <button className="btn" onClick={loadOHLC}>Reload</button>
              </div>
            </div>

            <div className="panelBody">
              <div className="chartWrap">
                <CandleChart candles={candles} showEMA={showEMA} emaPeriod={20} />
              </div>
            </div>
          </div>

          {/* ── Right column: stats + live tape ─────────────────────── */}
          <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            <div className="panel">
              <div className="panelHeader">
                <div>
                  <div className="panelTitle">Metrics</div>
                  <div className="panelSub">Live rolling analytics</div>
                </div>
              </div>
              <div className="panelBody">
                <div className="kpis">
                  <div className="kpi">
                    <div className="kpiLabel">Last close</div>
                    <div className="kpiValue">
                      {candles.length ? candles[candles.length - 1].close.toFixed(2) : "–"}
                    </div>
                  </div>
                  <div className="kpi">
                    <div className="kpiLabel">Volume (shown)</div>
                    <div className="kpiValue">
                      {candles.reduce((a, c) => a + (c.volume || 0), 0).toLocaleString()}
                    </div>
                  </div>
                  <div className="kpi">
                    <div className="kpiLabel">EMA</div>
                    <div className="kpiValue">
                      {metrics.ema != null ? metrics.ema.toFixed(2) : "–"}
                    </div>
                  </div>
                  <div className="kpi">
                    <div className="kpiLabel">VWAP</div>
                    <div className="kpiValue">
                      {metrics.vwap != null ? metrics.vwap.toFixed(2) : "–"}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <TradeTape symbol={symbol} mode="live" />
          </div>
        </div>
      </section>
    </div>
  )
}