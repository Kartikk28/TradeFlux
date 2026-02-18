import { useEffect, useMemo, useRef, useState } from "react";

type TapeTick = {
  ts: string;
  price: number;
  volume: number;
  symbol?: string;
};

export default function TradeTape({
  symbol,
  mode,
  replaySessionId,
  speed,
}: {
  symbol: string;
  mode: "live" | "replay";
  replaySessionId?: string | null;
  speed?: number;
}) {
  const [ticks, setTicks] = useState<TapeTick[]>([]);
  const [status, setStatus] = useState<string>("disconnected");
  const wsRef = useRef<WebSocket | null>(null);

  const wsUrl = useMemo(() => {
    if (mode === "replay") {
      if (!replaySessionId) return null;
      const sp = speed ?? 1;
      return `ws://127.0.0.1:8000/ws/replay?session_id=${encodeURIComponent(replaySessionId)}&speed=${sp}`;
    }
    return `ws://127.0.0.1:8000/ws/stream?symbol=${encodeURIComponent(symbol)}`;
  }, [mode, replaySessionId, speed, symbol]);

  useEffect(() => {
    if (!wsUrl) return;

    setTicks([]);
    setStatus("connecting");

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => setStatus("connected");
    ws.onclose = () => setStatus("disconnected");
    ws.onerror = () => setStatus("error");

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);

        if (msg?.type === "tick" && msg?.data) {
          const t = msg.data as TapeTick;
          setTicks((prev) => {
            const next = [t, ...prev];
            return next.slice(0, 200);
          });
          return;
        }

        if (msg?.ts && msg?.price != null) {
          const t = msg as TapeTick;
          setTicks((prev) => {
            const next = [t, ...prev];
            return next.slice(0, 200);
          });
          return;
        }
      } catch {
        return;
      }
    };

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [wsUrl]);

  return (
    <div className="panel">
      <div className="panelHeader">
        <div>
          <div className="panelTitle">Tape</div>
          <div className="panelSub">Mode {mode} · Status {status}</div>
        </div>
      </div>

      <div className="tape">
        {ticks.map((t, idx) => (
          <div key={idx} className="tapeRow">
            <div className="tapeTs">{new Date(t.ts).toLocaleTimeString()}</div>
            <div className="tapePx">{t.price.toFixed(2)}</div>
            <div className="tapeVol">{t.volume}</div>
          </div>
        ))}
        {ticks.length === 0 ? <div className="muted">No ticks yet</div> : null}
      </div>
    </div>
  );
}
