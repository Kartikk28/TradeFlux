"""
Mock tick generator that runs inside the FastAPI process.
Used when MARKET_DATA_PROVIDER=mock and no Kafka is available.
Generates random-walk price ticks for a fixed symbol universe,
persists them to the DB, and broadcasts over WebSocket.
"""

import asyncio
import random
from datetime import datetime, timezone

from .db import insert_tick
from .analytics import RollingAnalytics

SYMBOLS = ["ACME", "MSFT", "COG", "GOW", "TFLX"]
TICK_INTERVAL = 0.3  # seconds between batches (~3 ticks/sec total)


class MockIngestor:
    def __init__(self, manager):
        self.manager = manager
        self.analytics = RollingAnalytics()
        self.latencies: list[float] = []
        self._running = False
        self._prices: dict[str, float] = {
            s: 100.0 + 20 * random.random() for s in SYMBOLS
        }

    async def start(self):
        self._running = True
        print("MockIngestor: starting random-walk tick generator")
        while self._running:
            for symbol in SYMBOLS:
                await self._emit(symbol)
            await asyncio.sleep(TICK_INTERVAL)

    async def _emit(self, symbol: str):
        p = self._prices[symbol]
        p *= 1 + random.uniform(-0.002, 0.002)
        p = max(p, 1.0)
        self._prices[symbol] = p
        volume = random.randint(10, 500)
        now = datetime.now(timezone.utc)

        tick = {
            "symbol": symbol,
            "ts": now.isoformat(),
            "price": round(p, 4),
            "volume": volume,
        }

        try:
            await insert_tick(symbol, now, float(p), volume)
        except Exception as exc:
            print(f"MockIngestor: DB insert error for {symbol}: {exc}")
            return

        ema, vwap = self.analytics.update(symbol, float(p), volume)

        # track a zero latency (generated locally)
        self.latencies.append(0.0)
        self.latencies = self.latencies[-500:]

        await self.manager.broadcast(symbol, {
            "type": "tick",
            "tick": tick,
            "metrics": {"ema": ema, "vwap": vwap},
            "ingest_latency_ms": 0.0,
        })

    def stop(self):
        self._running = False

    def p50_p95(self):
        if not self.latencies:
            return (None, None)
        arr = sorted(self.latencies)

        def percentile(p):
            idx = min(max(int(len(arr) * p / 100), 0), len(arr) - 1)
            return arr[idx]

        return (percentile(50), percentile(95))
