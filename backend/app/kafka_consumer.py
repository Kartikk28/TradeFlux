
import asyncio
import json
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from .settings import settings
from .db import insert_tick
from .analytics import RollingAnalytics

RECONNECT_DELAY = 5  # seconds between reconnect attempts


class Ingestor:
    def __init__(self, manager):
        self.consumer: AIOKafkaConsumer | None = None
        self.manager = manager
        self.analytics = RollingAnalytics()
        self.latencies: list[float] = []

    async def start(self):
        """Connect to Kafka and consume ticks indefinitely.
        Retries on connection failure so the backend stays alive
        even when Kafka is temporarily unreachable."""
        while True:
            try:
                await self._run_once()
            except (KafkaConnectionError, Exception) as exc:
                print(f"Ingestor: Kafka error ({exc!r}), retrying in {RECONNECT_DELAY}s")
                await asyncio.sleep(RECONNECT_DELAY)

    async def _run_once(self):
        self.consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        await self.consumer.start()
        print(f"Ingestor: connected to Kafka @ {settings.kafka_bootstrap}")
        try:
            async for msg in self.consumer:
                tick = msg.value
                ts = datetime.fromisoformat(tick["ts"])
                now = datetime.now(timezone.utc)
                latency_ms = (now - ts).total_seconds() * 1000.0
                self.latencies.append(latency_ms)
                self.latencies = self.latencies[-500:]

                await insert_tick(tick["symbol"], ts, float(tick["price"]), int(tick["volume"]))
                ema, vwap = self.analytics.update(tick["symbol"], float(tick["price"]), int(tick["volume"]))

                await self.manager.broadcast(tick["symbol"], {
                    "type": "tick",
                    "tick": tick,
                    "metrics": {"ema": ema, "vwap": vwap},
                    "ingest_latency_ms": latency_ms,
                })
        finally:
            await self.consumer.stop()

    def p50_p95(self):
        if not self.latencies:
            return (None, None)
        arr = sorted(self.latencies)

        def percentile(p):
            idx = min(max(int(len(arr) * p / 100), 0), len(arr) - 1)
            return arr[idx]

        return (percentile(50), percentile(95))
