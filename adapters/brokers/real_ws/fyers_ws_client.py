import asyncio
from typing import Optional, Dict, List, Any
from fyers_apiv3.FyersWebsocket import data_ws
from upstox_client.rest import ApiException

from config import settings
from adapters.brokers.real_ws.broker_base_ws import BrokerBaseWS
from utils.logger import log


#TODO fix this

class FyersWebSocket(BrokerBaseWS):

    def __init__(self,app_id: Optional[str] = None,access_token: Optional[str] = None):
        super().__init__(broker_name="Fyers")
        self.app_id = app_id or settings.fyers_app_id
        self.access_token = access_token or settings.fyers_access_token
        self.fyers_ws = None

        if not self.access_token:
            raise ValueError("Fyers access token not provided!")
        if not self.app_id:
            raise ValueError("Fyers app_id not provided!")


    async def connect(self):
        try:
            log.info("Connecting to Fyers WebSocket...")

            self.fyers_ws = data_ws.FyersDataSocket(
                access_token=f"{self.app_id}:{self.access_token}",  # Access token in the format "appid:accesstoken"
                log_path="",  # Path to save logs. Leave empty to auto-create logs in the current directory.
                litemode=False,  # Lite mode disabled. Set to True if you want a lite response.
                write_to_file=False,  # Save response in a log file instead of printing it.
                reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
                on_connect=self.on_open,  # Callback function to subscribe to data upon connection.
                on_close=self.on_close,  # Callback function to handle WebSocket connection close events.
                on_error=self.on_error,  # Callback function to handle WebSocket errors.
                on_message=self.on_message  # Callback function to handle incoming messages from the WebSocket.
            )


            await asyncio.to_thread(self.fyers_ws.connect)

            # Wait a bit for connection to establish
            await asyncio.sleep(1)

            if self.is_connected:
                log.success("Connected to Fyers WebSocket")
            else:
                log.warning("Connected but status not confirmed")

        except ApiException as e:
            log.error(f"Fyers API Error: {e}")
            raise
        except Exception as e:
            log.error(f"Fyers Connection error: {e}")
            raise


    def on_error(self, error):
        log.error(f"Fyers WebSocket Error: {error}")
        self.is_connected = False
        self._handle_error(error)

    def on_close(self):
        log.warning("Fyers WebSocket connection closed")
        self.is_connected = False
        self._handle_close()

    def on_open(self):
        log.success("Fyers WebSocket connection opened")
        self.is_connected = True
        self.fyers_ws.keep_running()



    def on_message(self, message):

        try:
            # TODO handle the message received from upstox websocket
            log.info(f"Message received : {message}")
            # TODO call _parse_tick and then from there call _handle_tick()

            # Parse tick data
            # tick = self._parse_tick(data)
            #
            # if tick and self.on_tick_callback:
            #     # Update metrics
            #     self.update_last_message_time()
            #     self.increment_tick_count()
            #
            #     # Call callback
            #     asyncio.create_task(self._async_callback(tick))

        except Exception as e:
            log.error(f"Error parsing Fyers message: {e}")
            log.debug(f"Raw message received: {message}")

    async def subscribe(self, instruments: List[str]):
        # symbols: List of Fyers symbols (e.g., ['NSE:RELIANCE-EQ', 'NSE:INFY-EQ'])

        try:
            if not self.is_connected:
                raise RuntimeError("Not connected!  Call connect() first.")


            if not instruments:
                log.warning("No instruments provided to subscribe")
                return

            try:
                log.info(f"Subscribing to {len(instruments)} instruments...")

                # Fyers subscribe format
                await asyncio.to_thread(
                    self.fyers_ws.subscribe,
                    symbol=instruments,
                    data_type="SymbolUpdate"
                )

                self.subscribed_symbols.update(instruments)

                log.success(f"Fyers websocket started for {len(instruments)} instruments")
                for symbol in instruments:
                    log.info(f"   {symbol}")

            except Exception as e:
                log.error(f"Subscription error in Fyers websocket: {e}")
                raise
        except Exception as e:
            log.error(f"Subscription failed in fyers websocket: {e}")

    async def unsubscribe(self, symbols: List[str]) -> bool:
        """
        Unsubscribe from symbols

        Args:
            symbols: List of Fyers symbols

        Returns:
            True if unsubscribed successfully
        """
        if not self.is_connected:
            log.warning("Not connected, cannot unsubscribe")
            return False

        try:
            log.info(f"📊 Unsubscribing from {len(symbols)} symbols...")

            await asyncio.to_thread(
                self.fyers_ws.unsubscribe,
                symbol=symbols
            )

            self.subscribed_symbols.difference_update(symbols)

            log.success(f"Unsubscribed from {len(symbols)} symbols")
            return True

        except Exception as e:
            log.error(f"Unsubscribe error: {e}")
            return False

    async def disconnect(self) -> bool:
        """
        Disconnect from WebSocket

        Returns:
            True if disconnected successfully
        """
        if self.fyers_ws:
            try:
                log.info("🔌 Disconnecting from Fyers...")
                await asyncio.to_thread(self.fyers_ws.close_connection)
                log.success("Disconnected from Fyers")

                self.is_connected = False
                self.subscribed_symbols.clear()
                return True

            except Exception as e:
                log.error(f"Disconnect error: {e}")
                return False

        return True

    def _parse_tick(self, raw_data: Any) -> Optional[Dict]:
        """
        Parse Fyers tick data to standardized format

        Args:
            raw_data: Raw tick data from Fyers

        Returns:
            Standardized tick dictionary
        """
        try:
            # Fyers data structure (example - adjust based on actual format)
            symbol = raw_data.get('symbol', raw_data.get('name', ''))

            # Use base class method to create standardized tick
            return self.get_standardized_tick(
                symbol=self._extract_symbol(symbol),
                instrument_key=symbol,
                exchange=self._extract_exchange(symbol),
                price=raw_data.get('ltp', raw_data.get('last_price')),
                volume=raw_data.get('volume', raw_data.get('v')),
                bid=raw_data.get('bid'),
                ask=raw_data.get('ask'),
                open_interest=raw_data.get('oi'),
                raw_data=raw_data
            )

        except Exception as e:
            log.error(f"Error parsing Fyers tick: {e}")
            return None

    # ==================== HELPER METHODS ====================

    def _extract_symbol(self, full_symbol: str) -> str:
        """
        Extract symbol from full symbol
        Example: NSE:RELIANCE-EQ -> RELIANCE
        """
        if ':' in full_symbol:
            symbol = full_symbol.split(':')[1]
            if '-' in symbol:
                return symbol.split('-')[0]
            return symbol
        return full_symbol

    def _extract_exchange(self, full_symbol: str) -> str:
        """
        Extract exchange from full symbol
        Example: NSE:RELIANCE-EQ -> NSE
        """
        if ':' in full_symbol:
            return full_symbol.split(':')[0]
        return 'NSE'

    async def _async_callback(self, tick: Dict):
        """Async wrapper for callback"""
        if self.on_tick_callback:
            if asyncio.iscoroutinefunction(self.on_tick_callback):
                await self.on_tick_callback(tick)
            else:
                self.on_tick_callback(tick)