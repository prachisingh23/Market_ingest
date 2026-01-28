
import asyncio
import threading
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timezone

from infrastructure.ohlc import AtomicOhlcEngine
from adapters.brokers.factory.broker_factory import BrokerFactory
from infrastructure.writers.redis_writer import RedisWriter
from config import settings
from mail_sender.mail_sender import mail_success_default, mail_redis_crash_default
from utils.logger import log
from utils.validators import to_int, to_float, validate_tick_data
from utils.prometheus_metrics import get_metrics
from utils.tick_filter import TickFilter
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
TOKENS_PATH = BASE_DIR / "contract" / "tokens.txt"



@dataclass
class TickData:
    """Normalized tick data structure."""
    broker: str
    exchange: str
    token: str
    last_price: float
    volume: int
    timestamp_ms: int
    raw_tick: Dict[str, Any]


class IntegratedOhlcProcessor:
    """
    Production-grade processor that:
    1. Receives ticks from Upstox WebSocket
    2. Queues them for batched processing
    3. Updates OHLC using atomic Lua scripts
    4. Writes raw ticks + OHLC to Redis
    """

    def __init__(
        self,
        redis_url: str,
        ohlc_key_prefix: str = "nse_fo",
        redis_writer: Optional[RedisWriter] = None
    ):
        self.redis_url = redis_url
        self.ohlc_key_prefix = ohlc_key_prefix

        # Components
        self.ohlc_engine = AtomicOhlcEngine(
            redis_url=redis_url,
            key_prefix=ohlc_key_prefix,
            pool_size=20,
            enable_debug=False,
            on_error=self._trigger_alert,
        )

        self.redis_writer = redis_writer or RedisWriter(
            redis_url=redis_url,
            enabled=True,
            pipeline_batch_size=int(getattr(settings, "redis_pipeline_batch", 50)),
            flush_interval_ms=int(getattr(settings, "redis_flush_interval_ms", 50)),
            on_error=self._trigger_alert,
            circuit_breaker_enabled=getattr(settings, "circuit_breaker_enabled", True),
        )
        
        self.upstox_broker = None
        
        # Prometheus metrics (if enabled)
        self._prometheus_enabled = getattr(settings, "prometheus_enabled", True)
        if self._prometheus_enabled:
            self._metrics = get_metrics()
            log.info("Prometheus metrics enabled")
        else:
            self._metrics = None

        # Tick filter (if enabled)
        self._tick_filter_enabled = getattr(settings, "tick_filter_enabled", False)
        if self._tick_filter_enabled:
            self._tick_filter = TickFilter(
                sample_rate=getattr(settings, "tick_sample_rate", 1),
                min_price_change_pct=getattr(settings, "tick_min_price_change_pct", 0.0),
            )
            log.info("Tick filtering enabled")
        else:
            self._tick_filter = None

        # Queues
        queue_maxsize = int(getattr(settings, "queue_maxsize", 2_000_000))
        self.tick_queue = asyncio.Queue(maxsize=queue_maxsize)
        
        # Workers
        self.tick_workers = []
        self.num_tick_workers = int(getattr(settings, "tick_worker_count", 24))
        
        # Metrics
        self.ticks_received = 0
        self.ticks_processed = 0
        self.ohlc_updates = 0
        self.errors = 0
        
        # Control
        self._stop_event = asyncio.Event()
        self._writer_ready = asyncio.Event()
        self._alert_sent = False
        
        log.info(
            f"IntegratedOhlcProcessor initialized | "
            f"workers={self.num_tick_workers} | "
            f"queue_size={queue_maxsize:,}"
        )
    
    def tick_handler(self, raw_tick: Dict[str, Any]) -> None:
        """
        Fast-path tick handler called by Upstox WebSocket.
        Just queue it - no processing here.
        """
        try:
            self.tick_queue.put_nowait(raw_tick)
            self.ticks_received += 1

            # Update Prometheus metrics
            if self._metrics:
                self._metrics.counter_inc("ticks_received_total")
                self._metrics.gauge_set("queue_size", self.tick_queue.qsize())
        except asyncio.QueueFull:
            self.errors += 1
            if self._metrics:
                self._metrics.counter_inc("queue_full_errors")

    def error_handler(self, error: Any) -> None:
        """WebSocket error handler."""
        log.error(f"❌ Broker error: {error}")
        self.errors += 1
        if not self._stop_event.is_set():
            self._trigger_alert(
                "broker runtime failure",
                "Broker runtime failure while engine running",
                str(error),
            )
            self.request_stop()
    
    def close_handler(self) -> None:
        """WebSocket close handler."""
        log.warning("WebSocket closed")
        if not self._stop_event.is_set():
            self._trigger_alert(
                "connection dropped while engine running",
                "WebSocket disconnected while engine running",
                "WebSocket closed unexpectedly",
            )
            self.request_stop()
    
    def request_stop(self) -> None:
        """Request graceful shutdown."""
        if not self._stop_event.is_set():
            log.warning("Stop requested")
            self._stop_event.set()
    
    async def _ensure_writer_ready(self) -> None:
        """Initialize Redis writer and OHLC engine."""
        if self._writer_ready.is_set():
            return
        
        log.info("Initializing Redis components...")
        
        # Initialize OHLC engine (loads Lua scripts)
        await self.ohlc_engine.initialize()
        
        # Start Redis writer workers
        await self.redis_writer.get_client()
        await self.redis_writer.start_workers()
        
        self._writer_ready.set()
        log.success("✅ Redis components ready")
    
    async def _tick_worker(self) -> None:
        """
        High-performance tick worker that:
        1. Batches ticks from queue
        2. Processes each tick through OHLC engine
        3. Writes both raw tick + OHLC update to Redis
        """
        # Cache settings to avoid repeated getattr in hot loop
        batch_size = getattr(settings, "tick_batch_size", 500)
        tick_q = self.tick_queue
        
        try:
            while not self._stop_event.is_set():
                batch = []
                
                # Get first tick (blocking)
                first = await tick_q.get()
                batch.append(first)
                
                # Grab available ticks up to batch size
                available = min(batch_size - 1, tick_q.qsize())
                for _ in range(available):
                    try:
                        batch.append(tick_q.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                
                # Process batch
                for raw_tick in batch:
                    await self._process_single_tick(raw_tick)
                    tick_q.task_done()
                
                self.ticks_processed += len(batch)
                
        except asyncio.CancelledError:
            log.info("Tick worker cancelled")
            raise
        except Exception as e:
            log.error(f"Tick worker error: {e}", exc_info=True)
    
    async def _process_single_tick(self, raw_tick: Dict[str, Any]) -> None:
        """
        Process a single tick:
        1. Apply filters (if enabled)
        2. Normalize tick data
        3. Update OHLC atomically
        4. Write raw tick to Redis
        """
        try:
            # Apply tick filter (if enabled)
            if self._tick_filter and not self._tick_filter.should_process(raw_tick):
                if self._metrics:
                    self._metrics.counter_inc("ticks_filtered")
                return

            # Extract normalized data
            exchange = raw_tick.get("exchange")
            token = raw_tick.get("token")
            last_price = to_float(raw_tick.get("last_price"))
            timestamp_ms = to_int(raw_tick.get("timestamp_ms"))
            
            # Validate all required fields in one call
            if not validate_tick_data(exchange, token, last_price, timestamp_ms):
                if self._metrics:
                    self._metrics.counter_inc("ticks_invalid")
                return

            # Get volume with default 0 if missing
            volume = to_int(raw_tick.get("volume")) or 0

            # Update OHLC atomically with Lua script
            candle = await self.ohlc_engine.update_tick(
                exchange=exchange,
                token=token,
                ltp=last_price,
                volume=volume,
                timestamp_ms=timestamp_ms
            )
            
            if candle:
                self.ohlc_updates += 1
                await self.redis_writer.write_ohlc(
                    {
                        "exchange": exchange,
                        "token": token,
                        "minute_code": candle.minute_int,
                        "open": candle.open,
                        "high": candle.high,
                        "low": candle.low,
                        "close": candle.close,
                        "volume": candle.volume,
                    }
                )

                # Update Prometheus metrics
                if self._metrics:
                    self._metrics.counter_inc("ohlc_updates_total")

            # Write raw tick to Redis (batched by writer)
            await self.redis_writer.write_tick(raw_tick)
            
            # Update Prometheus metrics
            if self._metrics:
                self._metrics.counter_inc("ticks_processed_total")

        except Exception as e:
            self.errors += 1
            if self._metrics:
                self._metrics.counter_inc("processing_errors")
            if self.errors % 100 == 0:  # Log only every 100th error to reduce overhead
                log.error(f"❌ Tick processing error: {e}")
            if self.errors == 1:  # Send alert only on first error
                self._trigger_alert(
                    "processing failure",
                    "Processing error during live tick handling",
                    str(e),
                )

    async def _metrics_reporter(self) -> None:
        """Periodic metrics reporter."""
        interval = 5.0
        
        while not self._stop_event.is_set():
            await asyncio.sleep(interval)
            
            # Calculate rates
            rate = (self.ticks_processed - getattr(self, '_last_processed', 0)) / interval
            self._last_processed = self.ticks_processed
            
            ohlc_stats = self.ohlc_engine.get_stats()
            redis_stats = self.redis_writer.get_stats()
            
            log.info(
                f"📊 METRICS | "
                f"Ticks: {self.ticks_processed:,} ({rate:.0f}/s) | "
                f"OHLC: {ohlc_stats['updates']:,} | "
                f"Queue: {self.tick_queue.qsize():,} | "
                f"Redis: {redis_stats['total_ohlc']:,} | "
                f"Errors: {self.errors}"
            )
    
    async def subscribe(self, instruments: list[str]) -> None:
        """Subscribe to instruments."""
        if not self.upstox_broker or not self.upstox_broker.is_connected:
            log.error("Cannot subscribe: broker not connected")
            return
        
        try:
            await self.upstox_broker.subscribe(instruments)
            log.success(f"✅ Subscribed to {len(instruments)} instruments")
        except Exception as e:
            log.error(f"Failed to subscribe: {e}")
    
    async def start(self) -> None:
        """Start the integrated OHLC processor."""
        self._stop_event.clear()
        
        # Start Prometheus server (if enabled)
        if self._prometheus_enabled and self._metrics:
            prom_port = getattr(settings, "prometheus_port", 9090)
            prom_host = getattr(settings, "prometheus_host", "0.0.0.0")
            try:
                await self._metrics.start_server(host=prom_host, port=prom_port)
            except Exception as e:
                log.warning(f"Failed to start Prometheus server: {e}")

        # Initialize Redis components
        await self._ensure_writer_ready()
        
        # Start tick workers
        log.info(f" Starting {self.num_tick_workers} tick workers...")
        for _ in range(self.num_tick_workers):
            worker = asyncio.create_task(self._tick_worker())
            self.tick_workers.append(worker)
        
        # Start metrics reporter
        metrics_task = asyncio.create_task(self._metrics_reporter())
        
        # Connect to Upstox
        try:
            self.upstox_broker = BrokerFactory.create_broker(
                broker_name="upstox",
                access_token=settings.upstox_access_token
            )
            
            if not self.upstox_broker:
                log.error("Unable to create Upstox broker")
                return
            
            self.upstox_broker.set_on_tick(self.tick_handler)
            self.upstox_broker.set_on_error(self.error_handler)
            self.upstox_broker.set_on_close(self.close_handler)
            
            await self.upstox_broker.connect()
            
            # Wait for connection
            retry_count = 0
            while not self.upstox_broker.is_connected and retry_count < 60:
                await asyncio.sleep(0.1)
                retry_count += 1
            
            if not self.upstox_broker.is_connected:
                log.error("❌ Failed to connect to Upstox")
                self._trigger_alert(
                    "broker authentication / connection failed",
                    "Broker authentication failed during startup",
                    "Upstox connect failed (likely token expired / invalid). Check logs for 401 Unauthorized.",
                )
                return
            
            # Subscribe to instruments
            with TOKENS_PATH.open("r") as f:
                instruments = [line.strip() for line in f if line.strip()]

            
            await self.upstox_broker.subscribe(instruments)
            
            log.success(
                f" System ready | "
                f"Workers: {self.num_tick_workers} | "
                f"Instruments: {len(instruments)}"
            )

            mail_success_default(
                broker="UPSTOX",
                token="TOKEN_OK",
                redis_status="OK"
            )
            
            # Wait for stop signal
            await self._stop_event.wait()
            
        finally:
            metrics_task.cancel()
            await self._shutdown()
    
    async def _shutdown(self) -> None:
        """Graceful shutdown."""
        log.warning("Initiating graceful shutdown...")
        
        # Disconnect WebSocket
        if self.upstox_broker:
            try:
                await self.upstox_broker.disconnect()
            except Exception as e:
                log.error(f"Error disconnecting broker: {e}")
        
        # Drain queue
        log.info("⏳ Draining tick queue...")
        await self.tick_queue.join()
        
        # Cancel workers
        for worker in self.tick_workers:
            worker.cancel()
        await asyncio.gather(*self.tick_workers, return_exceptions=True)
        
        # Flush Redis
        await self.redis_writer.flush()
        
        # Close components
        await self.redis_writer.close()
        await self.ohlc_engine.close()
        
        # Final stats
        ohlc_stats = self.ohlc_engine.get_stats()
        redis_stats = self.redis_writer.get_stats()
        
        log.info("📊 FINAL STATISTICS:")
        log.info(f"   • Ticks Received: {self.ticks_received:,}")
        log.info(f"   • Ticks Processed: {self.ticks_processed:,}")
        log.info(f"   • OHLC Updates: {ohlc_stats['updates']:,}")
        log.info(f"   • Redis Writes: {redis_stats['total_ohlc']:,}")
        log.info(f"   • Errors: {self.errors}")

        log.success("Shutdown complete")

    def _trigger_alert(self, reason: str, context: str, details: str) -> None:
        if self._alert_sent:
            return
        self._alert_sent = True
        timestamp = datetime.now(timezone.utc).isoformat()
        broker_name = getattr(self.upstox_broker, "broker_name", "UPSTOX")
        error_message = (
            f"Timestamp: {timestamp}\n"
            f"Broker: {broker_name}\n"
            f"Redis: {self.redis_url}\n"
            f"Reason: {reason}\n"
            f"Details: {details}"
        )
        threading.Thread(
            target=mail_redis_crash_default,
            args=(self.redis_url, error_message, context),
            daemon=True,
        ).start()
