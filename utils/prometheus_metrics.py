"""
Prometheus metrics exporter for monitoring.
"""
import asyncio
import time
from typing import Dict, Optional
from collections import defaultdict
from aiohttp import web
from utils.logger import setup_logger
logger = setup_logger("prometheus")
class PrometheusMetrics:
    """Prometheus metrics collector."""
    def __init__(self, namespace: str = "market_ingest"):
        self.namespace = namespace
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._app: Optional[web.Application] = None
        logger.info(f"Prometheus metrics initialized")
    def counter_inc(self, name: str, value: float = 1.0) -> None:
        """Increment counter."""
        self._counters[name] += value
    def gauge_set(self, name: str, value: float) -> None:
        """Set gauge value."""
        self._gauges[name] = value
    def export(self) -> str:
        """Export Prometheus format."""
        lines = []
        for name, value in self._counters.items():
            lines.append(f"# TYPE {self.namespace}_{name} counter")
            lines.append(f"{self.namespace}_{name} {value}")
        for name, value in self._gauges.items():
            lines.append(f"# TYPE {self.namespace}_{name} gauge")
            lines.append(f"{self.namespace}_{name} {value}")
        return "\n".join(lines) + "\n"
    async def _metrics_handler(self, request: web.Request) -> web.Response:
        return web.Response(text=self.export(), content_type="text/plain")
    async def start_server(self, host: str = "0.0.0.0", port: int = 9090) -> None:
        self._app = web.Application()
        self._app.router.add_get("/metrics", self._metrics_handler)
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        logger.info(f"✅ Prometheus at http://{host}:{port}/metrics")
_metrics = None
def get_metrics() -> PrometheusMetrics:
    global _metrics
    if _metrics is None:
        _metrics = PrometheusMetrics()
    return _metrics
