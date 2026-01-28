from abc import ABC, abstractmethod
from typing import Optional, Callable, List, Any, Dict
import asyncio
import inspect

from utils.logger import log


class  BrokerBaseWS(ABC):
    def __init__(self, broker_name: str):
        self.broker_name = broker_name.upper()
        self.is_connected = False
        self.subscribed_symbols = set()
        self.on_tick_callback: Optional[Callable] = None
        self.on_error_callback: Optional[Callable] = None
        self.on_close_callback: Optional[Callable] = None

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def disconnect(self):
        pass

    @abstractmethod
    async def subscribe(self, instruments: List[str]):
        pass

    @abstractmethod
    async def unsubscribe(self, instruments: List[str]) -> bool:
        pass

    @abstractmethod
    def _parse_tick(self, raw_data: Any) -> Optional[Dict]:
        pass

    def set_on_tick(self, callback: Callable):
        log.info(f"{self.broker_name} set on_tick callback: {callback}")
        self.on_tick_callback = callback

    def set_on_error(self, callback: Callable):
        self.on_error_callback = callback

    def set_on_close(self, callback: Callable):
        self.on_close_callback = callback

    def _handle_tick(self, tick_data):
        if self.on_tick_callback:
            # Support both sync and async callbacks
            try:
                if inspect.iscoroutinefunction(self.on_tick_callback):
                    asyncio.create_task(self.on_tick_callback(tick_data))
                else:
                    # call synchronously; if it returns a coroutine schedule it
                    result = self.on_tick_callback(tick_data)
                    if inspect.isawaitable(result):
                        asyncio.create_task(result)
            except Exception as e:
                log.error(f"Error invoking on_tick_callback: {e}")

    def _handle_error(self, error):
        log.error(f"{self.broker_name} error: {error}")
        if self.on_error_callback:
            try:
                if inspect.iscoroutinefunction(self.on_error_callback):
                    asyncio.create_task(self.on_error_callback(error))
                else:
                    result = self.on_error_callback(error)
                    if inspect.isawaitable(result):
                        asyncio.create_task(result)
            except Exception as e:
                log.error(f"Error invoking on_error_callback: {e}")

    def _handle_close(self):
        log.warning(f"{self.broker_name} connection closed")
        if self.on_close_callback:
            try:
                if inspect.iscoroutinefunction(self.on_close_callback):
                    asyncio.create_task(self.on_close_callback())
                else:
                    result = self.on_close_callback()
                    if inspect.isawaitable(result):
                        asyncio.create_task(result)
            except Exception as e:
                log.error(f"Error invoking on_close_callback: {e}")

    def __repr__(self):
        return f"<{self.broker_name} WS connected={self.is_connected}, symbols={len(self.subscribed_symbols)}>"
