from typing import Optional, Dict, List, Any, Callable
from dataclasses import dataclass

from utils.redis_manager import RedisManager
from infrastructure.ohlc.lua_scripts import SCRIPT_REGISTRY, SCRIPT_SHAS
from infrastructure.ohlc.time_utils import TimeUtil
from utils.logger import log


@dataclass
class OhlcCandle:
    """OHLC candle data structure."""
    exchange: str
    token: str
    minute_int: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp_ms: int
    closed: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "exchange": self.exchange,
            "token": self.token,
            "minute_int": self.minute_int,
            "minute_str": TimeUtil.format_minute_int(self.minute_int),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timestamp_ms": self.timestamp_ms,
            "closed": self.closed,
        }
    
    @staticmethod
    def from_redis_list(
        exchange: str,
        token: str,
        redis_list: List[bytes],
        closed: bool = False
    ) -> Optional['OhlcCandle']:
        """Parse Redis list [o, h, l, c, volume, minute_int] into OhlcCandle."""
        if len(redis_list) < 6:
            return None

        try:
            return OhlcCandle(
                exchange=exchange,
                token=token,
                open=float(redis_list[0]),
                high=float(redis_list[1]),
                low=float(redis_list[2]),
                close=float(redis_list[3]),
                volume=int(float(redis_list[4])),
                minute_int=int(redis_list[5]),
                timestamp_ms=TimeUtil.minute_int_to_timestamp_ms(int(redis_list[5])),
                closed=closed,
            )
        except (ValueError, IndexError) as e:
            log.error(f"Failed to parse OHLC from Redis: {e}")
            return None


class AtomicOhlcEngine:
    """
    Production-grade OHLC engine with Lua atomic operations.
    
    Key Architecture:
    - All updates use Lua scripts (atomic)
    - Dual snapshot: _latest_ (running) + _confirmed_ (closed)
    - Historical candles: nse_fo:{token}:{minute_int}
    - O(1) read/write operations
    """
    
    def __init__(
        self,
        redis_url: str,
        key_prefix: str = "nse_fo",
        pool_size: int = 20,
        enable_debug: bool = False,
        on_error: Optional[Callable[[str, str, str], None]] = None,
    ):
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.pool_size = pool_size
        self.enable_debug = enable_debug
        self._on_error = on_error
        
        self._client = None
        self._scripts_loaded = False
        
        # Performance metrics
        self.updates_count = 0
        self.candles_closed = 0
        self.errors_count = 0
        
    async def _get_client(self):
        """Get or create Redis client using centralized manager."""
        if self._client is None:
            self._client = await RedisManager.get_client(
                self.redis_url,
                pool_size=self.pool_size,
                decode_responses=False,
                name="ohlc-engine"
            )
            log.success("✅ OHLC Engine connected to Redis")
        return self._client
    
    async def _load_lua_scripts(self) -> None:
        """Load Lua scripts into Redis and cache their SHAs."""
        if self._scripts_loaded:
            return
        
        client = await self._get_client()
        
        log.info("Loading Lua scripts into Redis...")
        for name, script in SCRIPT_REGISTRY.items():
            try:
                sha = await client.script_load(script)
                SCRIPT_SHAS[name] = sha
                log.success(f"   ✅ {name}: {sha[:8]}...")
            except Exception as e:
                log.error(f"   ❌ Failed to load {name}: {e}")
                raise
        
        self._scripts_loaded = True
        log.success(f"✅ {len(SCRIPT_SHAS)} Lua scripts loaded")
    
    async def initialize(self) -> None:
        """Initialize the engine (connect + load scripts)."""
        await self._get_client()
        await self._load_lua_scripts()
        log.success("AtomicOhlcEngine ready")
    
    async def update_tick(
        self,
        exchange: str,
        token: str,
        ltp: float,
        volume: int,
        timestamp_ms: int
    ) -> Optional[OhlcCandle]:
        """
        Atomically update OHLC with a new tick using Lua script.
        
        This method:
        1. Calculates minute_int from timestamp
        2. Executes atomic Lua script that:
           - Checks for minute rollover
           - Closes previous candle if needed
           - Updates or creates _latest_ candle
        3. Returns the updated candle
        
        Args:
            exchange: Exchange name (e.g., "NSE_INDEX")
            token: Instrument token
            ltp: Last traded price
            volume: Tick volume
            timestamp_ms: Tick timestamp in milliseconds
        
        Returns:
            Updated OhlcCandle or None on error
        """
        try:
            minute_int = TimeUtil.timestamp_ms_to_minute_int(timestamp_ms)
            
            if minute_int == 0:
                return None
            
            client = await self._get_client()
            
            # Execute atomic Lua script
            sha = SCRIPT_SHAS.get("atomic_update")
            if not sha:
                log.error("Lua script not loaded!")
                return None
            
            result = await client.evalsha(
                sha,
                0,
                self.key_prefix,
                str(token),
                str(minute_int),
                str(ltp),
                str(volume),
                str(timestamp_ms)
            )
            
            self.updates_count += 1
            
            # Parse result [o, h, l, c, volume, minute_int]
            if result and len(result) >= 6:
                candle = OhlcCandle.from_redis_list(
                    exchange=exchange,
                    token=token,
                    redis_list=result,
                    closed=False
                )
                
                if self.enable_debug and self.updates_count % 1000 == 0:
                    log.debug(
                        f"{exchange}:{token} @ {TimeUtil.format_minute_int(minute_int)} | "
                        f"O={candle.open} H={candle.high} L={candle.low} C={candle.close}"
                    )
                
                return candle
            
            return None
            
        except Exception as e:
            self.errors_count += 1
            # Check if it's a Redis-specific error
            if "redis" in type(e).__module__.lower():
                log.error(f"❌ Redis error in update_tick: {e}")
                self._notify_error(
                    "Redis Lua update failure",
                    "Redis unavailable during live tick processing",
                    str(e),
                )
            else:
                log.error(f"❌ Unexpected error in update_tick: {e}")
                self._notify_error(
                    "Tick processing failure",
                    "Processing error during live tick handling",
                    str(e),
                )
            return None
    
    async def get_latest_candle(
        self,
        exchange: str,
        token: str
    ) -> Optional[OhlcCandle]:
        """
        Get the currently running candle (_latest_).
        
        Returns:
            OhlcCandle or None if no active candle
        """
        try:
            client = await self._get_client()
            key = f"{self.key_prefix}:{token}:_latest_:"

            result = await client.lrange(key, 0, -1)
            
            if not result or len(result) < 6:
                return None
            
            return OhlcCandle.from_redis_list(
                exchange=exchange,
                token=token,
                redis_list=result,
                closed=False
            )
        except Exception as e:
            log.error(f"❌ Error fetching latest candle: {e}")
            return None
    
    async def get_confirmed_candle(
        self,
        exchange: str,
        token: str
    ) -> Optional[OhlcCandle]:
        """
        Get the last confirmed (closed) candle (_confirmed_).
        
        Returns:
            OhlcCandle or None if no confirmed candle
        """
        try:
            client = await self._get_client()
            key = f"{self.key_prefix}:{token}:_confirmed_:"

            result = await client.lrange(key, 0, -1)
            
            if not result or len(result) < 6:
                return None
            
            return OhlcCandle.from_redis_list(
                exchange=exchange,
                token=token,
                redis_list=result,
                closed=True
            )
        except Exception as e:
            log.error(f"❌ Error fetching confirmed candle: {e}")
            return None

    def _notify_error(self, reason: str, context: str, details: str) -> None:
        if not self._on_error:
            return
        try:
            self._on_error(reason, context, details)
        except Exception:
            log.exception("Failed to notify alert handler")
    
    async def get_historical_candle(
        self,
        exchange: str,
        token: str,
        minute_int: int
    ) -> Optional[OhlcCandle]:
        """
        Get a specific historical candle by minute_int.
        
        Args:
            exchange: Exchange name
            token: Instrument token
            minute_int: Minute code (e.g., 91500 for 9:15)
        
        Returns:
            OhlcCandle or None if not found
        """
        try:
            client = await self._get_client()
            key = f"{self.key_prefix}:{token}:{minute_int}:"

            result = await client.lrange(key, 0, -1)
            
            if not result or len(result) < 5:
                return None

            # Historical keys have [o, h, l, c, volume], add minute_int
            extended_result = result + [str(minute_int).encode()]
            
            return OhlcCandle.from_redis_list(
                exchange=exchange,
                token=token,
                redis_list=extended_result,
                closed=True
            )
        except Exception as e:
            log.error(f"❌ Error fetching historical candle: {e}")
            return None
    
    async def get_historical_range(
        self,
        exchange: str,
        token: str,
        start_minute: int,
        end_minute: int
    ) -> List[OhlcCandle]:
        """
        Get historical candles in a time range using Lua script.
        
        Args:
            exchange: Exchange name
            token: Instrument token
            start_minute: Start minute_int (e.g., 91500)
            end_minute: End minute_int (e.g., 92000)
        
        Returns:
            List of OhlcCandles sorted by time
        """
        try:
            client = await self._get_client()
            sha = SCRIPT_SHAS.get("historical_range")
            
            if not sha:
                log.error("Lua script not loaded!")
                return []
            
            result = await client.evalsha(
                sha,
                0,
                self.key_prefix,
                token,
                str(start_minute),
                str(end_minute)
            )
            
            candles = []
            if result:
                for item in result:
                    # item = {minute, ohlcv}
                    minute = int(item[0])
                    ohlcv = item[1]
                    
                    if len(ohlcv) >= 5:
                        extended = ohlcv + [str(minute).encode()]
                        candle = OhlcCandle.from_redis_list(
                            exchange=exchange,
                            token=token,
                            redis_list=extended,
                            closed=True
                        )
                        if candle:
                            candles.append(candle)
            
            return candles
            
        except Exception as e:
            log.error(f"❌ Error fetching historical range: {e}")
            return []
    
    async def force_close_candle(
        self,
        exchange: str,
        token: str
    ) -> bool:
        """
        Manually close the current candle (for testing or EOD).
        
        Returns:
            True if closed successfully
        """
        try:
            client = await self._get_client()
            sha = SCRIPT_SHAS.get("force_close")
            
            if not sha:
                log.error("Lua script not loaded!")
                return False
            
            result = await client.evalsha(
                sha,
                0,
                self.key_prefix,
                token
            )
            
            # Result is a dict-like structure from Lua
            if result and isinstance(result, (list, dict)):
                self.candles_closed += 1
                log.info(f"✅ Force closed candle for {exchange}:{token}")
                return True
            
            return False
            
        except Exception as e:
            log.error(f"❌ Error force closing candle: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get engine performance statistics."""
        return {
            "updates": self.updates_count,
            "candles_closed": self.candles_closed,
            "errors": self.errors_count,
            "scripts_loaded": self._scripts_loaded,
            "key_prefix": self.key_prefix,
        }
    
    async def close(self) -> None:
        """Close Redis connections (managed by RedisManager)."""
        self._client = None
        log.info(
            f"📊 OHLC Engine closed | Updates: {self.updates_count:,} | "
            f"Candles: {self.candles_closed:,} | Errors: {self.errors_count}"
        )
