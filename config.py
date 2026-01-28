from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent  # directory where config.py is


class Settings(BaseSettings):
    feed_source: str
    redis_url: str
    redis_enabled: bool = True
    redis_pipeline_batch_size: int = 20
    redis_pipeline_enabled: bool = True
    redis_pipeline_batch: int = 50
    redis_flush_interval_ms: int = 50
    upstox_market_data_feed_realtime_url: str
    fyers_realtime_url: str
    dhan_live_market_feed_realtime_url: str
    broker_failover_order: list[str] = ["UPSTOX", "FYERS", "DHAN"]
    upstox_access_token:str
    fyers_access_token:str
    fyers_app_id:str
    mail_from: str
    mail_from_name: str
    mail_api_key:str

    # Logging Settings
    log_level: str = "TRACE"
    log_rotation: str = "10 MB"
    log_directory: str = "logs"
    log_file: str = "app.log"
    log_retention: str = "1 days"

    # Circuit Breaker Settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 30.0

    # Prometheus Metrics
    prometheus_enabled: bool = True
    prometheus_port: int = 9090
    prometheus_host: str = "0.0.0.0"

    # Tick Filtering
    tick_filter_enabled: bool = False
    tick_sample_rate: int = 1  # 1=all, 10=every 10th
    tick_min_price_change_pct: float = 0.0

    # Multi-Process Scaling
    num_processes: int = 1  # 1=single process, 2+=multi-process

    # Worker Settings
    tick_worker_count: int = 24
    queue_maxsize: int = 2_000_000

    model_config = SettingsConfigDict(
        # env_file=".env",
        env_file=str(BASE_DIR / ".env"),
        case_sensitive=False,  # REDIS_URL and redis_url both work
        extra="ignore",  # Ignore unknown fields in config files
        validate_default=True,  # Validate default values
        frozen=True,  # Make settings immutable after creation
    )


settings = Settings()
