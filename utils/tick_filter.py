"""
Configurable tick filtering and sampling.
"""
import random
from typing import Dict, Set, Optional, Callable
from utils.logger import setup_logger
logger = setup_logger("tick-filter")
class TickFilter:
    """
    Filter and sample ticks based on configuration.
    Features:
    - Token whitelist/blacklist
    - Exchange filtering
    - Sampling (every Nth tick)
    - Price change threshold
    """
    def __init__(
        self,
        whitelist_tokens: Optional[Set[str]] = None,
        blacklist_tokens: Optional[Set[str]] = None,
        whitelist_exchanges: Optional[Set[str]] = None,
        sample_rate: int = 1,  # 1 = all, 10 = every 10th
        min_price_change_pct: float = 0.0,  # 0 = all, 0.1 = 0.1% change
    ):
        self.whitelist_tokens = whitelist_tokens
        self.blacklist_tokens = blacklist_tokens or set()
        self.whitelist_exchanges = whitelist_exchanges
        self.sample_rate = max(1, sample_rate)
        self.min_price_change_pct = min_price_change_pct
        # Track last prices for change detection
        self._last_prices: Dict[str, float] = {}
        self._tick_counter = 0
        # Stats
        self.total_ticks = 0
        self.filtered_ticks = 0
        self.sampled_ticks = 0
        logger.info(
            f"TickFilter initialized | "
            f"sample_rate=1:{sample_rate} | "
            f"min_change={min_price_change_pct}%"
        )
    def should_process(self, tick: Dict) -> bool:
        """
        Check if tick should be processed.
        Returns:
            True if tick passes all filters
        """
        self.total_ticks += 1
        exchange = tick.get("exchange", "")
        token = tick.get("token", "")
        ltp = tick.get("last_price", 0.0)
        # Exchange filter
        if self.whitelist_exchanges and exchange not in self.whitelist_exchanges:
            self.filtered_ticks += 1
            return False
        # Token whitelist (if set, only these pass)
        if self.whitelist_tokens and token not in self.whitelist_tokens:
            self.filtered_ticks += 1
            return False
        # Token blacklist
        if token in self.blacklist_tokens:
            self.filtered_ticks += 1
            return False
        # Sampling
        self._tick_counter += 1
        if self._tick_counter % self.sample_rate != 0:
            self.sampled_ticks += 1
            return False
        # Price change threshold
        if self.min_price_change_pct > 0:
            key = f"{exchange}:{token}"
            if key in self._last_prices:
                last_price = self._last_prices[key]
                if last_price > 0:
                    change_pct = abs((ltp - last_price) / last_price) * 100
                    if change_pct < self.min_price_change_pct:
                        self.filtered_ticks += 1
                        return False
            self._last_prices[key] = ltp
        return True
    def get_stats(self) -> Dict:
        """Get filter statistics."""
        return {
            "total_ticks": self.total_ticks,
            "filtered_ticks": self.filtered_ticks,
            "sampled_ticks": self.sampled_ticks,
            "passed_ticks": self.total_ticks - self.filtered_ticks - self.sampled_ticks,
            "filter_rate": f"{(self.filtered_ticks / max(1, self.total_ticks)) * 100:.2f}%",
        }
