# TradeFlux

**Real-time market-data streaming terminal.** Ticks flow from a producer → Kafka → FastAPI backend → PostgreSQL, while a React/Vite frontend renders live candlestick charts, a trade tape, and rolling analytics (EMA, VWAP) over WebSockets.

---

## Table of contents

1. [Architecture overview](#architecture-overview)
2. [Repository layout](#repository-layout)
3. [Quick start (Docker Compose)](#quick-start-docker-compose)
4. [Local development (without Docker)](#local-development-without-docker)
5. [Environment variables reference](#environment-variables-reference)
6. [API reference](#api-reference)
7. [Frontend](#frontend)
8. [Deploying to Vercel (frontend) + Railway (backend)](#deploying)
9. [Adding real market data via Alpaca](#alpaca)
10. [Working on this project later](#working-on-this-project-later)

---

## Architecture overview

```
┌──────────────────────────────────────────────────────────────────┐
│                         TradeFlux system                          │
│                                                                  │
│  ┌──────────────┐        ┌──────────┐       ┌──────────────┐    │
│  │ tick_producer│──tick─▶│  Kafka   │──msg─▶│ kafka_       │    │
│  │  (mock/real) │        │  topic:  │       │ consumer     │    │
│  └──────────────┘        │ tradeflux│       │ (Ingestor)   │    │
│                          │  .ticks  │       └──────┬───────┘    │
│                          └──────────┘              │            │
│                                              write │ broadcast  │
│                                                    ▼            │
│                                        ┌───────────────────┐    │
│                                        │   PostgreSQL DB    │    │
│                                        │  tables: ticks,    │    │
│                                        │  replay_sessions   │    │
│                                        └───────────────────┘    │
│                                                    ▲            │
│                                               read │            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  FastAPI Backend (:8000)                  │   │
│  │  REST: /api/health  /api/symbols  /api/ohlc  /api/metrics │   │
│  │  REST: POST /api/replay/start                            │   │
│  │  WS:  /ws/stream?symbol=<sym>                            │   │
│  │  WS:  /ws/replay?session_id=<id>&speed=<n>               │   │
│  └──────────────────────────────────────────────────────────┘   │
│                          ▲  WebSocket                            │
│                          │                                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │          React / Vite Frontend (:5173)                    │   │
│  │  • Sidebar: symbol list, time-window selector             │   │
│  │  • CandleChart: OHLCV + EMA-20 overlay (lightweight-charts│   │
│  │  • Metrics panel: last close, volume, EMA, VWAP           │   │
│  │  • TradeTape: live scrolling tick feed                    │   │
│  └──────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Repository layout

```
tradeflux/
├── backend/                  FastAPI service
│   ├── app/
│   │   ├── main.py           Routes, WebSocket handlers, startup
│   │   ├── settings.py       Pydantic-settings config (env vars)
│   │   ├── db.py             Async SQLAlchemy helpers (insert/query)
│   │   ├── models.py         Pydantic models (Tick, Candle, Metrics)
│   │   ├── kafka_consumer.py Ingestor: consumes Kafka → DB → WS broadcast
│   │   ├── analytics.py      Rolling EMA & VWAP per symbol
│   │   ├── ws_manager.py     WebSocket connection registry
│   │   └── alpaca_ingestor.py Optional: stream real trades from Alpaca
│   ├── Dockerfile
│   └── requirements.txt
│
├── frontend/                 React / Vite app
│   ├── src/
│   │   ├── App.tsx           Root component, state, WebSocket hook
│   │   ├── main.tsx          ReactDOM entry point
│   │   ├── styles.css        Dark-mode design system (CSS vars)
│   │   ├── lib/api.ts        API base-URL helper
│   │   └── components/
│   │       ├── CandleChart.tsx  lightweight-charts OHLCV + EMA
│   │       ├── Chart.tsx        Simple chart (alternative)
│   │       ├── TradeTape.tsx    Live / replay tick feed
│   │       └── TickerTable.tsx  Generic label/value table
│   ├── vercel.json           Vercel SPA deployment config
│   ├── vite.config.ts        Vite + dev-proxy config
│   ├── package.json
│   └── Dockerfile
│
├── producer/                 Mock tick generator
│   ├── tick_producer.py      Random-walk price simulation → Kafka
│   ├── Dockerfile
│   └── requirements.txt
│
├── infra/
│   └── docker-compose.yml    Full stack: PG + Zookeeper + Kafka + services
│
├── scripts/
│   └── init.sql              PostgreSQL schema (ticks, replay_sessions)
│
├── .env.example              Copy to .env and fill in secrets
├── .gitignore
└── README.md                 ← you are here
```

---

## Quick start (Docker Compose)

> **Prerequisites:** Docker ≥ 24, Docker Compose ≥ 2

```bash
# 1. Clone
git clone https://github.com/Kartikk28/TradeFlux.git
cd TradeFlux

# 2. Create your env file
cp .env.example .env        # edit if needed – defaults work out of the box

# 3. Launch everything
cd infra
docker compose up --build

# Services:
#   Frontend  →  http://localhost:5173
#   Backend   →  http://localhost:8000
#   Kafka     →  localhost:29092  (for debug tools)
#   Postgres  →  localhost:5432   (user/pass: tradeflux/tradeflux)
```

The `tick_producer` starts generating random-walk ticks for symbols
`ACME MSFT COG GOW TFLX` immediately. Open http://localhost:5173 to see them.

---

## Local development (without Docker)

### 1 – Start infrastructure only

```bash
cd infra

# Spin up only Postgres + Kafka (not the app services)
docker compose up postgres zookeeper kafka kafka-init -d
```

### 2 – Backend

```bash
cd backend

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Create a .env in the backend folder (or export vars directly)
export DB_URL="postgresql+asyncpg://tradeflux:tradeflux@localhost:5432/tradeflux"
export KAFKA_BOOTSTRAP="localhost:29092"
export KAFKA_TOPIC="tradeflux.ticks"
export ALLOW_CORS="true"

uvicorn app.main:app --reload --port 8000
```

### 3 – Producer

```bash
cd producer
pip install -r requirements.txt

KAFKA_BOOTSTRAP=localhost:29092 python tick_producer.py
```

### 4 – Frontend

```bash
cd frontend
npm install

# Set your backend URL (defaults to http://localhost:8000)
echo "VITE_API_URL=http://localhost:8000" > .env.local
npm run dev
# → http://localhost:5173
```

---

## Environment variables reference

| Variable | Default | Description |
|---|---|---|
| `DB_URL` | — | asyncpg PostgreSQL connection string |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `tradeflux.ticks` | Topic name |
| `ALLOW_CORS` | `true` | Enable CORS wildcard (disable in prod) |
| `MARKET_DATA_PROVIDER` | `mock` | `mock` or `alpaca` |
| `ALPACA_KEY_ID` | — | Alpaca API key (only when provider=alpaca) |
| `ALPACA_SECRET_KEY` | — | Alpaca API secret |
| `ALPACA_STREAM_URL` | `wss://…/v2/iex` | Alpaca WebSocket stream URL |
| `ALPACA_SYMBOLS` | `AAPL,MSFT,TSLA` | Symbols to subscribe to |
| `VITE_API_URL` | `http://localhost:8000` | Backend URL, read by the frontend |

---

## API reference

### REST

| Method | Path | Query params | Description |
|---|---|---|---|
| GET | `/api/health` | — | Returns `status: ok` + ingest latency p50/p95 |
| GET | `/api/symbols` | — | List all symbols with ticks in DB |
| GET | `/api/ohlc` | `symbol`, `window` (e.g. `15m`), `limit` | OHLCV candles for a symbol |
| GET | `/api/metrics` | `symbol` | Rolling EMA + VWAP for a symbol |
| POST | `/api/replay/start` | `symbol`, `start_ts`, `end_ts` | Creates a replay session, returns `session_id` |

### WebSocket

| Path | Params | Description |
|---|---|---|
| `/ws/stream` | `symbol` | Live tick stream. Sends `{type:"tick", tick:…, metrics:…, ingest_latency_ms:…}` |
| `/ws/replay` | `session_id`, `speed` | Streams historical ticks at `speed`× real-time |

---

## Frontend

The React app is a single-page application built with **Vite + TypeScript**. Key components:

- **`App.tsx`** – top-level shell, manages WebSocket lifecycle, polls OHLC
- **`CandleChart.tsx`** – wraps [lightweight-charts](https://tradingview.github.io/lightweight-charts/) v4 with candlestick + volume histogram + EMA-20 line
- **`TradeTape.tsx`** – scrolling real-time (or replay) trade feed
- **`styles.css`** – full dark-mode design system using CSS custom properties

**EMA toggle** – checkbox in the sidebar controls the `showEMA` prop passed to `CandleChart`.

---

## Deploying

### Frontend → Vercel

1. Import the repo into **Vercel** (`vercel.com/new`).
2. Set **Root Directory** to `frontend`.
3. Add environment variable `VITE_API_URL` pointing to your deployed backend URL.
4. Deploy — Vercel auto-detects Vite and builds `npm run build`.

```
Root Directory:   frontend
Build Command:    npm run build
Output Directory: dist
```

### Backend → Railway (or Render / Fly.io)

The backend needs persistent services (Postgres + Kafka) so it cannot run on Vercel serverless. Recommended options:

| Platform | Notes |
|---|---|
| **Railway** | Add Postgres and Kafka plugins, set env vars, point to `backend/` |
| **Render** | Web service + managed Postgres; use Upstash Kafka for managed Kafka |
| **Fly.io** | `fly launch` inside `backend/`; provision Upstash Kafka add-on |

Once the backend is deployed, update `VITE_API_URL` in Vercel to the new backend URL and redeploy the frontend.

> **WebSockets on Vercel** — Vercel serverless functions do not support WebSockets. The backend must be deployed on a long-running server (Railway, Render, Fly.io, EC2, etc.).

---

## Adding real market data via Alpaca

1. Sign up at [alpaca.markets](https://alpaca.markets) (free IEX feed available).
2. Set in your `.env`:
   ```
   MARKET_DATA_PROVIDER=alpaca
   ALPACA_KEY_ID=your_key
   ALPACA_SECRET_KEY=your_secret
   ALPACA_SYMBOLS=AAPL,TSLA,MSFT
   ```
3. The `AlpacaStreamIngestor` in `backend/app/alpaca_ingestor.py` connects to Alpaca's WebSocket trade feed and writes ticks directly to Postgres.

> **Note:** `alpaca_ingestor.py` currently persists ticks to DB only. To add WS broadcasting (same as the Kafka path), pass the `WSManager` instance to it and call `manager.broadcast()` after `insert_tick()`.

---

## Working on this project later

### Chain of thought for adding a feature

1. **Define the data shape** – add a Pydantic model in `models.py` if needed.
2. **Add DB query** – write the SQL helper in `db.py` (async, typed).
3. **Expose REST or WS endpoint** – add route in `main.py`, wire up the DB/analytics call.
4. **Add frontend state** – update `App.tsx` state + `useEffect` hooks.
5. **Add/update component** – create or edit a component in `src/components/`.
6. **Test locally** – `docker compose up` or run each service separately.
7. **Push** – commit and push; Vercel redeploys the frontend automatically.

### Suggested next features (priority order)

| Feature | Where to implement |
|---|---|
| Alert system (price threshold) | Backend: new `alerts` table + REST; Frontend: alert modal |
| Orderbook ladder | Producer: add bid/ask; Backend: new WS message type; Frontend: new component |
| Watchlist / favorites | Frontend only: `localStorage`-backed list |
| Keyboard shortcuts | Frontend: `useEffect` with `keydown` listener |
| Dark/light theme toggle | Frontend: CSS var override via `data-theme` attribute |
| TimescaleDB hypertable | `scripts/init.sql`: `SELECT create_hypertable('ticks','ts')` |
| Authentication | Backend: FastAPI OAuth2 + JWT; Frontend: login page |
| Historical data import | New script in `scripts/`: fetch from Alpaca history API, bulk insert |

### Common debugging tips

```bash
# Check backend logs
docker compose -f infra/docker-compose.yml logs -f backend

# Peek at ticks flowing through Kafka
docker compose -f infra/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic tradeflux.ticks

# Connect to Postgres directly
psql postgresql://tradeflux:tradeflux@localhost:5432/tradeflux -c "SELECT COUNT(*) FROM ticks;"

# Rebuild a single service after code changes
docker compose -f infra/docker-compose.yml up --build backend
```

---

## License

MIT
