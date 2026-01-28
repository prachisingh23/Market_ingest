from typing import List, Optional, Dict, Any
import upstox_client
from upstox_client.rest import ApiException
from config import settings
from adapters.brokers.real_ws.broker_base_ws import BrokerBaseWS
from utils.validators import to_int, to_float, sanitize_token
from utils.logger import log
import asyncio


class UpstoxWebSocket(BrokerBaseWS):

    # access token can be provided in class or can be obtained from settings
    def __init__(self, access_token: Optional[str] = None):
        super().__init__("Upstox")
        self.access_token = access_token or settings.upstox_access_token
        self.streamer = None
        self.configuration = None
        self.api_client = None
        self.mode = "ltpc"

        if not self.access_token:
            raise ValueError("Upstox access token not provided!")

    @staticmethod
    def _sanitize_token(token: str) -> str:
        """
        Sanitize token for Redis key safety.
        Replaces spaces and special chars with underscores.

        Examples:
        - "Nifty Bank" -> "Nifty_Bank"
        - "Nifty 50" -> "Nifty_50"
        - "Bank Nifty" -> "Bank_Nifty"
        """
        if not token:
            return token

        # Replace spaces with underscores
        sanitized = token.replace(" ", "_")

        # Replace other problematic chars for Redis keys
        # Redis keys can contain: A-Z, a-z, 0-9, ., -, _
        # Replace anything else with underscore
        safe_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-")
        sanitized = "".join(c if c in safe_chars else "_" for c in sanitized)

        return sanitized


    async def connect(self):
        try:
            log.info("Connecting to Upstox WebSocket...")

            # Configure API client
            self.configuration = upstox_client.Configuration()
            self.configuration.access_token = self.access_token
            self.api_client = upstox_client.ApiClient(self.configuration)
            self.streamer = upstox_client.MarketDataStreamerV3(self.api_client)

            # Ensure SDK callbacks run in the asyncio event loop
            loop = asyncio.get_running_loop()

            def _marshal(fn):
                def _handler(*args, **kwargs):
                    # schedule the bound method to run in the asyncio loop thread-safely
                    loop.call_soon_threadsafe(fn, *args, **kwargs)
                return _handler

            # Set event handlers/listeners (marshal into asyncio loop)
            self.streamer.on("message", _marshal(self.on_message))
            self.streamer.on("error", _marshal(self.on_error))
            self.streamer.on("close", _marshal(self.on_close))
            self.streamer.on("open", _marshal(self.on_open))

            await asyncio.to_thread(self.streamer.connect)

            # Wait a bit for connection to establish
            await asyncio.sleep(1)

            if self.is_connected:
                log.success(f"Connected to Upstox WebSocket")
            else:
                log.warning("Connected but status not confirmed")


        except ApiException as e:
            log.error(f"Upstox API Error: {e}")
            raise
        except Exception as e:
            log.error(f"Connection error: {e}")
            raise

    def on_error(self, error, *args, **kwargs):
        log.error(f"Upstox WebSocket Error: {error}")
        self.is_connected = False
        self._handle_error(error)

    def on_close(self, code=None, reason=None, *args, **kwargs):
        log.warning(f"Upstox WebSocket closed | Code: {code} | Reason: {reason}")
        self.is_connected = False
        self._handle_close()



    def on_open(self, *args, **kwargs):
        log.success("Upstox WebSocket connection opened")
        self.is_connected = True

    # Message is already decoded from Protobuf by the SDK.
    def on_message(self, message, *args, **kwargs):
        try:
            log.info(f"Message received : {message}")
            ticks = self._parse_tick(message)
            if not ticks:
                return

            for tick in ticks:
                if self.on_tick_callback:
                    self._handle_tick(tick)

        except Exception as e:
            log.error(f"Error parsing Upstox message: {e}")
            log.debug(f"Raw message received: {message}")


    def _parse_tick(self, raw_data: Any) -> Optional[List[Dict]]:
        try:
            if not isinstance(raw_data, dict):
                return None

            log.info(f"Received a raw tick now parsing here, {raw_data}")
            feeds = raw_data.get("feeds", {})
            if not feeds:
                return None

            ticks = []
            source_type = raw_data.get("type") or None
            for instrument_key, feed_data in feeds.items():
                try:
                    prices = feed_data.get("ltpc",{})

                    # CRITICAL: Only process ticks with LTP (real-time price)
                    # Skip CP-ONLY ticks (pre-market / no trading data)
                    ltp = to_float(prices.get("ltp"))
                    if ltp is None:
                        # CP-ONLY tick → Skip it
                        log.debug(f"Skipping CP-only tick: {instrument_key}")
                        continue

                    # Additional safety: Skip invalid prices
                    if ltp <= 0:
                        log.warning(f"Skipping invalid LTP {ltp} for {instrument_key}")
                        continue

                    timestamp = to_int(prices.get("ltt"))
                    if "|" in instrument_key:
                        exchange, token = instrument_key.split("|", 1)
                    else:
                        exchange, token = None, instrument_key

                    # Sanitize token for Redis key safety
                    # "Nifty Bank" -> "Nifty_Bank", "Nifty 50" -> "Nifty_50"
                    token = sanitize_token(token or instrument_key)

                    # Get volume with default 0 if missing
                    volume = to_int(prices.get("ltq"))
                    if volume is None:
                        volume = 0

                    tick = {
                        "broker": "UPSTOX",
                        "exchange": exchange,
                        "token": token,
                        "last_price": ltp,  # ✅ Only use real LTP, never fallback to CP
                        "prev_close": to_float(prices.get("cp")),
                        "volume": volume,  # ✅ Default to 0 if missing
                        "timestamp_ms": timestamp,
                        "source_type": source_type
                    }
                    ticks.append(tick)
                except Exception as e:
                    log.error(f"Failed to parse feed for {instrument_key}: {e}")
                    continue

            return ticks

        except Exception as e:
            log.error(f"Tick parse error: {e}")
            return None

    async def subscribe(self, instruments: List[str], mode: str = "ltpc"):

        try:
            if not self.is_connected:
                raise RuntimeError("Not connected!  Call connect() first.")

            if not instruments:
                log.warning("No instruments provided to subscribe")
                return

            try:
                log.info(f"Subscribing to {len(instruments)} instruments in '{mode}' mode...")
                self.mode = mode

                # Subscribe using Upstox SDK
                await asyncio.to_thread(self.streamer.subscribe, instruments, self.mode)

                # Update our tracking
                self.subscribed_symbols.update(instruments)

                log.success(f"Upstox WebSocket streamer started for {len(instruments)} instruments with mode {self.mode}")
                for symbol in instruments:
                    log.info(f"   {symbol}")

            except Exception as e:
                log.error(f"Subscription error in upstox websocket: {e}")
                raise
        except Exception as e:
            log.error(f"Subscription failed in upstox websocket: {e}")

    async def unsubscribe(self, instruments: List[str]):
        if not self.is_connected:
            log.warning("Not connected, cannot unsubscribe")
            return
        if not self.streamer:
            log.warning("Not connected, cannot unsubscribe")
            return

        try:
            log.info(f"Unsubscribing from {len(instruments)} symbols...")
            # Unsubscribe using Upstox SDK
            await asyncio.to_thread(self.streamer.unsubscribe,instruments)

            # Update our tracking
            self.subscribed_symbols.difference_update(instruments)

            log.success(f"Unsubscribed from {len(instruments)} symbols")
            for symbol in instruments:
                log.info(f"   {symbol}")


        except Exception as e:
            log.error(f"Upstox websocket Unsubscribe error: {e}")

    async def disconnect(self):
        try:
            if self.streamer:
               try:
                   log.info("Disconnecting from Upstox WebSocket...")
                   await asyncio.to_thread(self.streamer.disconnect)
                   log.success("Disconnected from Upstox")
               except Exception as e:
                   log.error(f"Error disconnecting: {e}")

            self.is_connected = False
            self.subscribed_symbols.clear()
        except Exception as e:
            log.error(f"Upstox websocket Disconnect error: {e}")

    # this function will return the connection status of websocket
    def get_status(self) -> Dict:
        return {
            'broker': 'upstox',
            'connected': self.is_connected,
            'mode': self.mode,
            'subscribed_symbols': list(self.subscribed_symbols),
            'symbol_count': len(self.subscribed_symbols)
        }
