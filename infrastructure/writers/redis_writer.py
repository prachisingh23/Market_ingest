import asyncio
import msgspec  # 5-7x faster than json!
from contextlib import suppress
from typing import Optional, Callable

from utils.redis_manager import RedisManager
from utils.logger import setup_logger
from utils.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpenError

logger = setup_logger("redis-writer")


class RedisWriter:
    """Async writer that persists normalized ticks to Redis.

    The writer will attempt to reconnect on every call to ``write_tick`` so that
    transient network issues or a restarted Redis instance do not bring down the
    ingestion pipeline. Payloads are serialized as JSON and pushed to the
    ``ticks`` list by default.
    """

    def __init__(
        self,
        redis_url: str,
        list_key: str = "ticks",
        ohlc_prefix: str = "ohlc:1m",
        *,
        enabled: bool = True,
        pipeline_batch_size: int = 1,
        flush_interval_ms: int = 100,
        on_error: Optional[Callable[[str, str, str], None]] = None,
        circuit_breaker_enabled: bool = True,
    ):
        self.redis_url = redis_url
        self.list_key = list_key
        self.ohlc_prefix = ohlc_prefix
        self.enabled = enabled
        self._client = None
        self._pipeline_batch_size = max(1, pipeline_batch_size)
        self._pipeline = None
        self._pipeline_lock = asyncio.Lock()
        self._pending_pipeline_ops = 0
        self._stats = {"ticks": 0, "ohlc": 0}
        self._tick_list_disabled = not list_key or list_key == "ticks"
        self._flush_interval = max(0, flush_interval_ms)
        self._flush_task: Optional[asyncio.Task] = None
        self._shutdown = False
        self._on_error = on_error

        # Circuit Breaker for Redis fault isolation
        self._circuit_breaker_enabled = circuit_breaker_enabled
        if circuit_breaker_enabled:
            self._circuit_breaker = CircuitBreaker(
                name="redis-writer",
                config=CircuitBreakerConfig(
                    failure_threshold=5,
                    timeout_seconds=30.0
                )
            )
        else:
            self._circuit_breaker = None

        if self._tick_list_disabled:
            logger.warning("Tick LIST persistence disabled; only OHLC hashes will be written.")

    async def _get_client(self):
        """Get or create Redis client using centralized manager."""
        if not self._client:
            self._client = await RedisManager.get_client(
                self.redis_url,
                pool_size=10,
                decode_responses=True,
                name="redis-writer"
            )
        return self._client

    async def _flush_pipeline(self) -> None:
        if self._pipeline is None or self._pending_pipeline_ops == 0:
            return

        try:
            # Use circuit breaker if enabled
            if self._circuit_breaker:
                async with self._circuit_breaker:
                    await self._pipeline.execute()
            else:
                await self._pipeline.execute()
        except CircuitBreakerOpenError as e:
            logger.warning(f"Circuit breaker OPEN - skipping pipeline flush: {e}")
            # Don't reset client - circuit will recover automatically
        except Exception as exc:  # pragma: no cover - protective logging
            logger.error(f"Failed to flush Redis pipeline: {exc}")
            self._notify_error(
                "Redis pipeline flush failure",
                "Redis unavailable during live tick processing",
                str(exc),
            )
            self._client = None
        finally:
            self._pipeline = None
            self._pending_pipeline_ops = 0

    async def _write_json(self, list_key: str, payload: dict) -> None:
        if not self.enabled or self._tick_list_disabled:
            return

        client = await self._get_client()
        try:
            if self._pipeline_batch_size == 1:
                await client.rpush(list_key, msgspec.json.encode(payload).decode('utf-8'))
            else:
                async with self._pipeline_lock:
                    if self._pipeline is None:
                        self._pipeline = client.pipeline(transaction=False)
                    self._pipeline.rpush(list_key, msgspec.json.encode(payload).decode('utf-8'))
                    self._pending_pipeline_ops += 1
                    if self._pending_pipeline_ops >= self._pipeline_batch_size:
                        await self._flush_pipeline()
        except Exception as exc:  # pragma: no cover - protective logging
            logger.error(f"Failed to write tick to Redis: {exc}")
            self._notify_error(
                "Redis tick write failure",
                "Redis unavailable during live tick processing",
                str(exc),
            )
            # Drop the client so the next attempt reconnects cleanly.
            self._client = None

    async def write_tick(self, tick: dict) -> None:
        """Persist a single normalized tick (disabled)."""

        if self._tick_list_disabled:
            return

        await self._write_json(self.list_key, tick)
        self._stats["ticks"] += 1

    async def write_ohlc(self, ohlc: dict) -> None:
        """Persist an OHLC snapshot update as a per-token hash."""
        if not self.enabled:
            return

        exchange = ohlc.get("exchange")
        token = ohlc.get("token")
        minute_code = ohlc.get("minute_code")

        if not exchange or not token or minute_code is None:
            logger.error(f"Skipping OHLC write due to missing key metadata: {ohlc}")
            return

        key = f"{self.ohlc_prefix}:{exchange}:{token}"

        # Pre-serialize values once (performance optimization with msgspec!)
        ohlc_values = [
            ohlc.get("open"),
            ohlc.get("high"),
            ohlc.get("low"),
            ohlc.get("close"),
            ohlc.get("volume"),
        ]
        # msgspec is 5-7x faster than json.dumps!
        ohlc_json = msgspec.json.encode(ohlc_values).decode('utf-8')
        latest_json = msgspec.json.encode(ohlc_values + [minute_code]).decode('utf-8')

        client = await self._get_client()
        try:
            if self._pipeline_batch_size == 1:
                # Direct write (no pipeline)
                await client.hset(
                    key,
                    mapping={
                        str(minute_code): ohlc_json,
                        "__latest__": latest_json,
                    },
                )
            else:
                # Batched pipeline write
                async with self._pipeline_lock:
                    if self._pipeline is None:
                        self._pipeline = client.pipeline(transaction=False)

                    self._pipeline.hset(
                        key,
                        mapping={
                            str(minute_code): ohlc_json,
                            "__latest__": latest_json,
                        },
                    )
                    self._pending_pipeline_ops += 1

                    if self._pending_pipeline_ops >= self._pipeline_batch_size:
                        await self._flush_pipeline()

            self._stats["ohlc"] += 1

        except Exception as exc:
            logger.error(f"Failed to write OHLC to Redis: {exc}")
            self._notify_error(
                "Redis OHLC write failure",
                "Redis unavailable during live tick processing",
                str(exc),
            )
            self._client = None

    async def flush(self) -> None:
        """Force any buffered pipeline operations to be sent."""

        if not self.enabled:
            return

        async with self._pipeline_lock:
            await self._flush_pipeline()

    async def get_client(self):
        """Compatibility helper for components expecting Redis client access."""
        return await self._get_client()

    async def start_workers(self) -> None:
        """Start background flush loop when batching is enabled."""
        if self._flush_task or self._flush_interval == 0 or self._pipeline_batch_size == 1:
            logger.info("RedisWriter simple mode - no background workers to start")
            return
        logger.info(
            "RedisWriter pipeline enabled (batch=%s, flush interval=%sms)",
            self._pipeline_batch_size,
            self._flush_interval,
        )
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def _flush_loop(self) -> None:
        if self._flush_interval == 0 or self._pipeline_batch_size == 1:
            return
        try:
            while not self._shutdown:
                await asyncio.sleep(self._flush_interval / 1000)
                await self.flush()
        except asyncio.CancelledError:
            pass

    def get_stats(self) -> dict:
        """Basic stats snapshot for metrics logs."""
        return {
            "enabled": self.enabled,
            "pipeline_batch_size": self._pipeline_batch_size,
            "pending_ops": self._pending_pipeline_ops,
            "connected": self._client is not None,
            "total_ticks": self._stats["ticks"],
            "total_ohlc": self._stats["ohlc"],
        }

    async def close(self) -> None:
        """Close Redis connection cleanly."""
        self._shutdown = True
        if self._flush_task:
            self._flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._flush_task
            self._flush_task = None
        if self._client:
            await self.flush()
            await self._client.close()
            self._client = None
            logger.info("RedisWriter connection closed")

    def _notify_error(self, reason: str, context: str, details: str) -> None:
        if not self._on_error:
            return
        try:
            self._on_error(reason, context, details)
        except Exception:
            logger.exception("Failed to notify alert handler")
