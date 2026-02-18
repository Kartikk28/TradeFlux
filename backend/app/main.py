from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import asyncio

from .settings import settings
from .db import (
    fetch_symbols,
    fetch_ohlc,
    create_replay_session,
    get_replay_session,
    stream_ticks,
)
from .ws_manager import WSManager
from .kafka_consumer import Ingestor


app = FastAPI(title="TradeFlux API")

if settings.allow_cors:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

manager = WSManager()
ingestor = Ingestor(manager)


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(ingestor.start())


@app.get("/api/health")
async def health():
    p50, p95 = ingestor.p50_p95()
    return {"status": "ok", "ingest_latency_ms": {"p50": p50, "p95": p95}}


@app.get("/api/symbols")
async def symbols():
    return {"symbols": await fetch_symbols()}


@app.get("/api/ohlc")
async def ohlc(symbol: str, window: str = "15m", limit: int = 200):
    data = await fetch_ohlc(symbol, window, limit)
    return {"symbol": symbol, "window": window, "candles": data}


@app.get("/api/metrics")
async def metrics(symbol: str):
    m = ingestor.analytics.get_metrics(symbol)
    return {"symbol": symbol, "metrics": m}


@app.post("/api/replay/start")
async def replay_start(symbol: str, start_ts: str | None = None, end_ts: str | None = None):
    try:
        s = datetime.fromisoformat(start_ts) if start_ts else None
        e = datetime.fromisoformat(end_ts) if end_ts else None
    except ValueError:
        raise HTTPException(status_code=400, detail="start_ts or end_ts is not valid ISO format")

    session_id = await create_replay_session(symbol, s, e)
    return {"session_id": session_id, "symbol": symbol, "start_ts": start_ts, "end_ts": end_ts}


@app.websocket("/ws/stream")
async def ws_stream(websocket: WebSocket, symbol: str):
    await manager.connect(symbol, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(symbol, websocket)


@app.websocket("/ws/replay")
async def ws_replay(websocket: WebSocket, session_id: str, speed: float = 1.0):
    await websocket.accept()

    if speed <= 0:
        speed = 1.0

    sess = await get_replay_session(session_id)
    if not sess:
        await websocket.send_json({"type": "error", "message": "Invalid session_id"})
        await websocket.close()
        return

    symbol = sess["symbol"]
    start_ts = sess["start_ts"]
    end_ts = sess["end_ts"]

    prev_dt = None

    try:
        async for tick in stream_ticks(symbol, start_ts=start_ts, end_ts=end_ts):
            dt = datetime.fromisoformat(tick["ts"].replace("Z", "+00:00"))

            if prev_dt is not None:
                gap = (dt - prev_dt).total_seconds()
                if gap > 0:
                    await asyncio.sleep(min(gap / speed, 0.25))

            prev_dt = dt
            await websocket.send_json({"type": "tick", "data": tick})

        await websocket.send_json({"type": "done"})
        await websocket.close()

    except WebSocketDisconnect:
        return
