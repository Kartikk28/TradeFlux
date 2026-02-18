from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
    )

    db_url: str
    allow_cors: bool = True

    # Kafka
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "tradeflux.ticks"

    # Market data configuration
    market_data_provider: str = "mock"  # mock or alpaca
    alpaca_key_id: Optional[str] = None
    alpaca_secret_key: Optional[str] = None
    alpaca_stream_url: str = "wss://stream.data.alpaca.markets/v2/iex"
    alpaca_symbols: str = "AAPL,MSFT,TSLA"

settings = Settings()
