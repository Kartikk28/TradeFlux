from __future__ import annotations

from typing import Any, AsyncGenerator, Optional
from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text
from .settings import settings

engine = create_async_engine(settings.db_url, future=True, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def insert_tick(symbol: str, ts, price: float, volume: int) -> None:
    async with SessionLocal() as session:
        await session.execute(
            text("INSERT INTO ticks(symbol, ts, price, volume) VALUES (:s, :t, :p, :v)"),
            {"s": symbol, "t": ts, "p": price, "v": volume},
        )
        await session.commit()


async def fetch_symbols(limit: int = 100) -> list[str]:
    async with SessionLocal() as session:
        rs = await session.execute(
            text("SELECT DISTINCT symbol FROM ticks ORDER BY symbol LIMIT :lim"),
            {"lim": limit},
        )
        return [r[0] for r in rs.all()]


async def fetch_ticks(symbol: str, limit: int = 200) -> list[dict[str, Any]]:
    async with SessionLocal() as session:
        rs = await session.execute(
            text(
                """
                SELECT ts, price, volume
                FROM ticks
                WHERE symbol = :sym
                ORDER BY ts DESC
                LIMIT :lim
                """
            ),
            {"sym": symbol, "lim": limit},
        )
        rows = rs.all()
        return [
            {"ts": r[0].isoformat(), "price": float(r[1]), "volume": int(r[2])}
            for r in reversed(rows)
        ]


async def create_replay_session(
    symbol: str,
    start_ts=None,
    end_ts=None,
) -> str:
    session_id = uuid4().hex
    async with SessionLocal() as session:
        await session.execute(
            text(
                """
                INSERT INTO replay_sessions(id, symbol, start_ts, end_ts)
                VALUES (:id, :sym, :start_ts, :end_ts)
                """
            ),
            {"id": session_id, "sym": symbol, "start_ts": start_ts, "end_ts": end_ts},
        )
        await session.commit()
    return session_id


async def get_replay_session(session_id: str) -> Optional[dict[str, Any]]:
    async with SessionLocal() as session:
        rs = await session.execute(
            text(
                """
                SELECT id, symbol, start_ts, end_ts
                FROM replay_sessions
                WHERE id = :id
                """
            ),
            {"id": session_id},
        )
        row = rs.first()
        if not row:
            return None
        return {"id": row[0], "symbol": row[1], "start_ts": row[2], "end_ts": row[3]}


async def stream_ticks(
    symbol: str,
    start_ts=None,
    end_ts=None,
) -> AsyncGenerator[dict[str, Any], None]:
    where = "symbol = :sym"
    params: dict[str, Any] = {"sym": symbol}

    if start_ts is not None:
        where += " AND ts >= :start_ts"
        params["start_ts"] = start_ts
    if end_ts is not None:
        where += " AND ts <= :end_ts"
        params["end_ts"] = end_ts

    sql = text(
        f"""
        SELECT ts, price, volume
        FROM ticks
        WHERE {where}
        ORDER BY ts ASC
        """
    )

    async with SessionLocal() as session:
        result = await session.stream(sql, params)
        async for row in result:
            yield {"ts": row[0].isoformat(), "price": float(row[1]), "volume": int(row[2]), "symbol": symbol}


async def fetch_ohlc(symbol: str, window: str = "15m", limit: int = 200):
    try:
        n = int(window[:-1])
        unit = window[-1]
    except Exception:
        n = 15
        unit = "m"

    if unit == "m":
        interval_sql = f"{n} minutes"
        date_trunc_unit = "minute"
    elif unit == "h":
        interval_sql = f"{n} hours"
        date_trunc_unit = "hour"
    elif unit == "d":
        interval_sql = f"{n} days"
        date_trunc_unit = "day"
    else:
        interval_sql = "15 minutes"
        date_trunc_unit = "minute"

    async with SessionLocal() as session:
        sql_date_bin = f"""
            WITH base AS (
                SELECT
                    date_bin(INTERVAL '{interval_sql}', ts, TIMESTAMP '1970-01-01') AS bucket,
                    ts,
                    price,
                    volume
                FROM ticks
                WHERE symbol = :sym
            ),
            agg AS (
                SELECT
                    bucket,
                    MAX(price) AS high,
                    MIN(price) AS low,
                    SUM(volume) AS volume
                FROM base
                GROUP BY bucket
            ),
            o AS (
                SELECT DISTINCT ON (bucket)
                    bucket,
                    price AS open
                FROM base
                ORDER BY bucket, ts ASC
            ),
            c AS (
                SELECT DISTINCT ON (bucket)
                    bucket,
                    price AS close
                FROM base
                ORDER BY bucket, ts DESC
            )
            SELECT
                agg.bucket,
                o.open,
                agg.high,
                agg.low,
                c.close,
                agg.volume
            FROM agg
            JOIN o USING (bucket)
            JOIN c USING (bucket)
            ORDER BY agg.bucket DESC
            LIMIT :lim
        """

        try:
            rs = await session.execute(text(sql_date_bin), {"sym": symbol, "lim": limit})
        except Exception:
            sql_trunc = f"""
                WITH base AS (
                    SELECT
                        date_trunc('{date_trunc_unit}', ts) AS bucket,
                        ts,
                        price,
                        volume
                    FROM ticks
                    WHERE symbol = :sym
                ),
                agg AS (
                    SELECT
                        bucket,
                        MAX(price) AS high,
                        MIN(price) AS low,
                        SUM(volume) AS volume
                    FROM base
                    GROUP BY bucket
                ),
                o AS (
                    SELECT DISTINCT ON (bucket)
                        bucket,
                        price AS open
                    FROM base
                    ORDER BY bucket, ts ASC
                ),
                c AS (
                    SELECT DISTINCT ON (bucket)
                        bucket,
                        price AS close
                    FROM base
                    ORDER BY bucket, ts DESC
                )
                SELECT
                    agg.bucket,
                    o.open,
                    agg.high,
                    agg.low,
                    c.close,
                    agg.volume
                FROM agg
                JOIN o USING (bucket)
                JOIN c USING (bucket)
                ORDER BY agg.bucket DESC
                LIMIT :lim
            """
            rs = await session.execute(text(sql_trunc), {"sym": symbol, "lim": limit})

        rows = rs.all()
        return [
            {
                "ts": r[0].isoformat(),
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": int(r[5]),
            }
            for r in reversed(rows)
        ]
