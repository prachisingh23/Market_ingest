from typing import Optional

from adapters.brokers.real_ws.broker_base_ws import BrokerBaseWS
from adapters.brokers.real_ws.fyers_ws_client import FyersWebSocket

from adapters.brokers.real_ws.upstox_ws_client import UpstoxWebSocket
from utils.logger import log


class BrokerFactory:
    @staticmethod
    def create_broker(broker_name: str, **credentials) -> Optional[BrokerBaseWS]:
        try:
            broker_name = broker_name.lower()
            log.info(f"Creating the {broker_name} websocket client")

            if broker_name=="upstox":
                access_token = credentials.get("access_token")
                return UpstoxWebSocket(access_token=access_token)
            elif broker_name=="fyers":
                access_token = credentials.get("access_token")
                app_id = credentials.get("app_id")
                return FyersWebSocket(access_token=access_token, app_id=app_id)
            else:
                log.error(f"{broker_name} is not supported!")
                return None
        except Exception as e:
            log.error(f"Error creating {broker_name} websocket client: {e}")
            return None


    @staticmethod
    def get_available_brokers() -> list:
        return ["Upstox", "Fyers"]


