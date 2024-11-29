# connection_handler.py
from paho.mqtt.client import ReasonCodes
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging


class ConnectionHandler(ABC):
    """Abstract base class for connection handling"""
    @abstractmethod
    def on_connect(self, client: Any, userdata: Any, flags: Dict, rc: int, properties: Optional[Dict] = None) -> None:
        pass

    @abstractmethod
    def on_disconnect(self, client: Any, userdata: Any, rc: int, properties: Optional[Dict] = None) -> None:
        pass


# connection_handler.py


class ConnectionHandler(ABC):
    """Abstract base class for connection handling"""
    @abstractmethod
    def on_connect(self, client: Any, userdata: Any, flags: Dict, rc: int, properties: Optional[Dict] = None) -> None:
        pass

    @abstractmethod
    def on_disconnect(self, client: Any, userdata: Any, rc: int, properties: Optional[Dict] = None) -> None:
        pass


class DefaultConnectionHandler(ConnectionHandler):
    """Default implementation of connection handling"""

    def __init__(self):
        self.connected = False
        self.subscribed_topics = set()

    def on_connect(self, client: Any, userdata: Any, flags: Dict, rc: Any, properties: Optional[Dict] = None) -> None:
        # For MQTT v5, rc is a ReasonCode object
        rc_value = rc.value if hasattr(rc, 'value') else rc

        connection_codes = {
            0: "Connection successful",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized"
        }

        if rc_value == 0:
            self.connected = True
            logging.info(f"Connected to HiveMQ Cloud: {connection_codes.get(rc_value, 'Unknown status')}")
            for topic in self.subscribed_topics:
                logging.info(f"Resubscribing to topic: {topic}")
                client.subscribe(topic)
        else:
            self.connected = False
            error_msg = connection_codes.get(
                rc_value, f"Unknown error code: {rc_value}")
            logging.error(f"Connection failed: {error_msg}")

    def on_disconnect(self, client: Any, userdata: Any, rc: int, properties: Optional[Dict] = None) -> None:
        self.connected = False
        if rc != 0:
            logging.warning(f"Unexpected disconnection (code: {rc})")
        logging.info("Disconnected from HiveMQ Cloud")
