
from pydantic import BaseModel, Field
from datetime import datetime

class Tick(BaseModel):
    symbol: str
    ts: datetime
    price: float
    volume: int

class Candle(BaseModel):
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class Metrics(BaseModel):
    symbol: str
    ema: float | None = None
    vwap: float | None = None
