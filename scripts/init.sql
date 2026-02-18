-- TradeFlux – PostgreSQL schema initialisation
-- Run automatically via docker-entrypoint-initdb.d or manually:
--   psql -U tradeflux -d tradeflux -f scripts/init.sql

-- ─── Extensions ────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ─── Core tick table ────────────────────────────────────────────────────────
-- Stores every raw trade tick ingested from Kafka / Alpaca.
-- For production scale, partition by ts (RANGE) and create a TimescaleDB
-- hypertable on this table instead.
CREATE TABLE IF NOT EXISTS ticks (
    id        BIGSERIAL,
    symbol    TEXT        NOT NULL,
    ts        TIMESTAMPTZ NOT NULL,
    price     NUMERIC(18, 6) NOT NULL,
    volume    INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

-- Default catch-all partition (covers all time up to 2100)
CREATE TABLE IF NOT EXISTS ticks_default
    PARTITION OF ticks DEFAULT;

-- Lookup index for symbol + time range queries (the hot path)
CREATE INDEX IF NOT EXISTS ticks_symbol_ts_idx ON ticks (symbol, ts DESC);

-- ─── Replay sessions ────────────────────────────────────────────────────────
-- Stores parameters for replay WebSocket sessions requested via
-- POST /api/replay/start.  Backend streams ticks back through
-- /ws/replay?session_id=<id>.
CREATE TABLE IF NOT EXISTS replay_sessions (
    id          TEXT        PRIMARY KEY,
    symbol      TEXT        NOT NULL,
    start_ts    TIMESTAMPTZ,
    end_ts      TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Auto-expire replay sessions older than 24 hours (optional cron job)
-- DELETE FROM replay_sessions WHERE created_at < NOW() - INTERVAL '24 hours';
