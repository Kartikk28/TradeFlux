import asyncio
import json
import os
from datetime import datetime, timezone

import websockets

from .db import insert_tick


class AlpacaStreamIngestor:
    def __init__(self):
        self.url = os.getenv("ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets/v2/test")
        self.key = os.getenv("ALPACA_KEY_ID", "")
        self.secret = os.getenv("ALPACA_SECRET_KEY", "")
        syms = os.getenv("ALPACA_SYMBOLS", "FAKEPACA")
        self.symbols = [s.strip().upper() for s in syms.split(",") if s.strip()]

        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            try:
                await self._run_once()
            except Exception as e:
                print("Alpaca stream error:", repr(e))
                await asyncio.sleep(2)

    async def _run_once(self):
        async with websockets.connect(self.url) as ws:
            # auth via message
            await ws.send(json.dumps({"action": "auth", "key": self.key, "secret": self.secret}))
            _ = await ws.recv()

            # subscribe to trades
            await ws.send(json.dumps({"action": "subscribe", "trades": self.symbols}))
            _ = await ws.recv()

            while self._running:
                msg = await ws.recv()
                data = json.loads(msg)

                # Alpaca sends arrays of events
                for evt in data:
                    if evt.get("T") != "t":
                        continue

                    symbol = evt.get("S")
                    price = evt.get("p")
                    size = evt.get("s", 0)

                    # timestamp can be nanoseconds or RFC3339 depending on feed
                    ts = evt.get("t")
                    if isinstance(ts, int):
                        dt = datetime.fromtimestamp(ts / 1_000_000_000, tz=timezone.utc)
                    else:
                        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

                    await insert_tick(symbol, dt, float(price), int(size))

    def stop(self):
        self._running = False
