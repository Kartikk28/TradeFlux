# TradeFlux – Architecture Deep Dive

> **Chain-of-thought walkthrough** of every component, every data flow, and every design decision so you can confidently extend the system.

---

## Table of contents

1. [System components](#1-system-components)
2. [Data flow – tick ingestion](#2-data-flow--tick-ingestion)
3. [Data flow – frontend live view](#3-data-flow--frontend-live-view)
4. [Data flow – replay](#4-data-flow--replay)
5. [Database schema](#5-database-schema)
6. [Backend internals](#6-backend-internals)
7. [Frontend internals](#7-frontend-internals)
8. [Analytics module](#8-analytics-module)
9. [Configuration & settings](#9-configuration--settings)
10. [Scalability considerations](#10-scalability-considerations)
11. [Deployment topology](#11-deployment-topology)

---

## 1. System components

| Component | Technology | Role |
|---|---|---|
| **tick_producer** | Python + `kafka-python` | Simulates a market data feed. Produces JSON tick messages to Kafka at ~3 ticks/sec across 5 symbols. |
| **Kafka** | Confluent Kafka 7.6 | Durable, ordered message bus. Decouples producers from consumers and allows fan-out. |
| **Zookeeper** | Confluent Zookeeper 7.6 | Kafka's coordination layer (will be replaced by KRaft in future Kafka versions). |
| **backend** | Python 3.11, FastAPI, asyncpg, SQLAlchemy 2.0 async | Core API server. Consumes Kafka, persists ticks, computes analytics, serves REST + WebSocket. |
| **PostgreSQL 16** | TimescaleDB-ready | Persistent tick store and replay session store. |
| **frontend** | React 18, Vite 5, TypeScript, lightweight-charts | Browser SPA. Connects via HTTP REST and WebSocket to the backend. |

---

## 2. Data flow – tick ingestion

```
tick_producer.py
│
│  Produces JSON every 0.3 s:
│  {"symbol":"ACME","ts":"2026-02-18T10:00:00Z","price":102.34,"volume":150}
│
▼
Kafka topic: tradeflux.ticks
│
▼
kafka_consumer.py  (Ingestor.start – asyncio task)
│
├── insert_tick() → PostgreSQL ticks table
│
├── analytics.update(symbol, price, volume)
│       └── Returns (ema, vwap)
│
└── ws_manager.broadcast(symbol, {type:"tick", tick, metrics, ingest_latency_ms})
        └── Sends JSON to every WebSocket client subscribed to that symbol
```

**Latency tracking:**  
The `Ingestor` records `(now_utc - tick.ts)` in milliseconds for each message. The last 500 values are kept and exposed via `GET /api/health` as p50 and p95 percentiles.

---

## 3. Data flow – frontend live view

```
Browser (App.tsx)
│
├── On mount: GET /api/symbols → populate sidebar list
│
├── On symbol/window change: GET /api/ohlc?symbol=…&window=… → CandleChart
│
├── Opens WebSocket: ws://backend/ws/stream?symbol=ACME
│   │
│   └── On message: update metrics state + refresh OHLC
│
└── Polls GET /api/health every 10 s → show p50 latency badge
```

**Why re-fetch OHLC on every tick?**  
The candlestick windows (1m, 15m, etc.) are computed server-side with SQL `date_bin`. The simplest correct approach is to re-query after each tick. A future optimisation is to push incremental candle updates from the server.

---

## 4. Data flow – replay

```
1. Client calls POST /api/replay/start?symbol=ACME&start_ts=…&end_ts=…
   → Backend creates row in replay_sessions, returns session_id

2. Client opens WebSocket: ws://backend/ws/replay?session_id=<id>&speed=2

3. Backend streams_ticks() from DB in ascending ts order
   For each tick:
     a. Compute wall-clock gap since previous tick
     b. asyncio.sleep(gap / speed) – capped at 250ms per step
     c. ws.send_json({type:"tick", data: tick})

4. When stream exhausted: ws.send_json({type:"done"}) → close
```

**Speed parameter:**  
`speed=1.0` → real-time replay.  
`speed=10.0` → 10× speed.  
`speed=0` → defaults to 1.0 (guard against division by zero).

---

## 5. Database schema

```sql
-- Core time-series table
CREATE TABLE ticks (
    id      BIGSERIAL,
    symbol  TEXT           NOT NULL,
    ts      TIMESTAMPTZ    NOT NULL,
    price   NUMERIC(18,6)  NOT NULL,
    volume  INTEGER        NOT NULL DEFAULT 0,
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);          -- range-partition by time for scale

CREATE INDEX ticks_symbol_ts_idx ON ticks (symbol, ts DESC);

-- Replay session metadata
CREATE TABLE replay_sessions (
    id          TEXT        PRIMARY KEY,   -- UUID hex
    symbol      TEXT        NOT NULL,
    start_ts    TIMESTAMPTZ,               -- NULL = from beginning
    end_ts      TIMESTAMPTZ,               -- NULL = up to now
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
```

**OHLC query design:**  
`db.fetch_ohlc()` uses PostgreSQL's `date_bin()` function to bucket ticks into arbitrary time windows. It falls back to `date_trunc()` for older PostgreSQL versions. The query uses four CTEs:
- `base` – raw ticks filtered by symbol
- `agg` – high, low, volume per bucket
- `o` – first price per bucket (open)
- `c` – last price per bucket (close)

---

## 6. Backend internals

### `main.py` – routes and lifecycle

```
app startup event
  └── asyncio.create_task(ingestor.start())   # fire-and-forget background task

GET  /api/health        → ingestor.p50_p95()
GET  /api/symbols       → db.fetch_symbols()
GET  /api/ohlc          → db.fetch_ohlc(symbol, window, limit)
GET  /api/metrics       → ingestor.analytics.get_metrics(symbol)
POST /api/replay/start  → db.create_replay_session()

WS /ws/stream   → WSManager.connect(symbol, ws); receive loop
WS /ws/replay   → stream_ticks() generator with sleep pacing
```

### `ws_manager.py` – connection registry

```python
# Dict[symbol → Set[WebSocket]]
# broadcast() iterates the set, collects broken sockets for cleanup
```

No external dependency – pure in-process state. **Caveat:** does not work across multiple backend processes. For multi-instance deployments, replace with Redis Pub/Sub.

### `kafka_consumer.py` – Ingestor

- Uses `aiokafka` (native asyncio Kafka client)
- `auto_offset_reset="latest"` – only processes ticks received after startup
- Latency ring buffer: keeps last 500 measurements (< 50 KB RAM)

### `analytics.py` – RollingAnalytics

```
EMA  : exponential moving average with α = 0.2 (configurable)
       ema_t = α × price_t + (1 − α) × ema_{t-1}

VWAP : volume-weighted average price over a rolling window
       VWAP = Σ(price × volume) / Σ(volume)  for last 200 ticks
```

Both are computed **in-process**, one value per symbol, per tick. This is O(1) per update.

---

## 7. Frontend internals

### Component tree

```
App
├── <aside> sidebar
│   ├── symbol search input
│   ├── window / limit selectors
│   ├── EMA toggle checkbox
│   └── symbol list (filtered)
│
└── <section> main
    ├── <header> topbar  (symbol name, LIVE badge, p50 latency)
    └── .grid
        ├── .panel  CandleChart (lightweight-charts)
        └── .column
            ├── .panel  Metrics (last close, volume, EMA, VWAP)
            └── TradeTape (live WS tick feed)
```

### State in `App.tsx`

| State | Type | Purpose |
|---|---|---|
| `symbols` | `string[]` | All symbols from DB |
| `symbol` | `string` | Currently selected symbol |
| `windowStr` | `string` | Candle time window (e.g. `"15m"`) |
| `limit` | `number` | Max candles to fetch |
| `candles` | `Candle[]` | Current OHLCV data |
| `metrics` | `{ema, vwap}` | Live rolling metrics |
| `latency` | `number\|null` | Ingest p50 latency in ms |
| `showEMA` | `boolean` | Toggle EMA overlay on chart |
| `status` | `"live"\|"idle"\|"error"` | WebSocket connection status |

### `CandleChart.tsx` internals

- Uses `lightweight-charts` v4 (TradingView)
- Three series layered on one chart instance:
  1. `CandlestickSeries` – OHLC bars
  2. `HistogramSeries` – volume bars (below price pane)
  3. `LineSeries` – EMA-20 calculated client-side
- EMA is computed in the browser with a standard exponential formula (matches the server-side value asymptotically)
- Chart is destroyed and recreated whenever `candles` prop changes (via `useEffect` cleanup)

### `TradeTape.tsx` internals

- Accepts `mode: "live" | "replay"`
- In live mode: connects to `/ws/stream?symbol=…`
- In replay mode: connects to `/ws/replay?session_id=…&speed=…`
- Maintains a rolling buffer of the last 200 ticks (via `setTicks(prev => [t, ...prev].slice(0, 200))`)

---

## 8. Analytics module

### EMA (Exponential Moving Average)

$$\text{EMA}_t = \alpha \cdot p_t + (1 - \alpha) \cdot \text{EMA}_{t-1}$$

- $\alpha = 0.2$ by default
- Fast response to price changes (higher $\alpha$) vs. smoother line (lower $\alpha$)
- Seeded with first price seen for each symbol

### VWAP (Volume-Weighted Average Price)

$$\text{VWAP} = \frac{\sum_{i=1}^{N} p_i \cdot v_i}{\sum_{i=1}^{N} v_i}$$

- Rolling window of $N = 200$ ticks per symbol
- `deque(maxlen=200)` automatically evicts oldest entries

Both metrics are exposed via `GET /api/metrics?symbol=…` and also pushed over WebSocket with every tick message.

---

## 9. Configuration & settings

`backend/app/settings.py` uses **Pydantic-Settings** which reads from:
1. Environment variables (highest priority)
2. `.env` file in the working directory
3. Pydantic field defaults

The `extra="forbid"` setting means any unknown env var named in the `.env` will raise a startup error – good for catching typos.

---

## 10. Scalability considerations

| Bottleneck | Current | Recommended upgrade |
|---|---|---|
| WS fan-out | In-process `Dict[symbol → Set[WS]]` | Redis Pub/Sub → multiple backend replicas |
| Tick storage | Single PostgreSQL table | TimescaleDB hypertable + retention policy |
| OHLC query | Full table scan per request | Materialized view refreshed every N seconds |
| Analytics | In-process per-symbol dict | Move to Redis or a stream-processing layer (Faust, Flink) |
| Kafka partitions | 1 | N partitions + N consumer replicas for horizontal scale |

---

## 11. Deployment topology

### Minimal production (single server)

```
                 ┌─────────────────────────────┐
                 │  Single VM / VPS             │
                 │                              │
                 │  docker-compose.yml          │
                 │  ┌────────┐  ┌───────────┐  │
                 │  │Postgres│  │   Kafka   │  │
                 │  └────────┘  └───────────┘  │
                 │  ┌────────┐  ┌───────────┐  │
                 │  │Backend │  │ Producer  │  │
                 │  │:8000   │  │           │  │
                 │  └────────┘  └───────────┘  │
                 └─────────────────────────────┘
                          ▲
                          │  HTTPS + WSS
                 ┌─────────────────────────────┐
                 │     Vercel (CDN Edge)         │
                 │     React SPA (static)        │
                 │     VITE_API_URL=https://…    │
                 └─────────────────────────────┘
```

### Scaled production

```
                 Vercel CDN  ──── React SPA (static)
                      │
                    HTTPS/WSS
                      │
                 Load Balancer (nginx / Cloudflare)
                      │
             ┌────────┴────────┐
          Backend-1          Backend-2
          FastAPI             FastAPI
             │                  │
          Redis Pub/Sub (shared WS fan-out)
             │
          Kafka cluster (3 brokers)
             │
          TimescaleDB (managed, e.g. Timescale Cloud)
```
