import paho.mqtt.client as mqtt
import time
from datetime import datetime
import json
import logging
from typing import Dict, Any, Optional

from mqtt_client.ssl_context import SSLContextFactory
from mqtt_client.message_handler import MessageHandler, Message
from mqtt_client.connection_handler import ConnectionHandler


class MQTTClientWrapper:
    """Main MQTT client wrapper implementing high-level MQTT operations"""

    def __init__(
        self,
        config: Dict[str, str],
        ssl_factory: SSLContextFactory,
        message_handler: MessageHandler,
        connection_handler: ConnectionHandler
    ):
        self.config = config
        self.message_handler = message_handler
        self.connection_handler = connection_handler

        self.client = mqtt.Client(
            client_id=config['client_id'],
            protocol=mqtt.MQTTv5,
            transport="tcp"
        )

        # Set up SSL
        self.client.tls_set_context(ssl_factory.create_ssl_context())

        # Set authentication
        self.client.username_pw_set(config['username'], config['password'])

        # Set callbacks
        self.client.on_connect = self.connection_handler.on_connect
        self.client.on_disconnect = self.connection_handler.on_disconnect
        self.client.on_message = self._on_message
        self.client.on_log = self._on_log

    def _on_message(self, client: Any, userdata: Any, msg: Any) -> None:
        try:
            message = Message(
                topic=msg.topic,
                payload=msg.payload.decode(),
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                qos=msg.qos
            )
            self.message_handler.handle_message(message)
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")

    def _on_log(self, client: Any, userdata: Any, level: int, buf: str) -> None:
        levels = {
            mqtt.MQTT_LOG_INFO: logging.INFO,
            mqtt.MQTT_LOG_NOTICE: logging.INFO,
            mqtt.MQTT_LOG_WARNING: logging.WARNING,
            mqtt.MQTT_LOG_ERR: logging.ERROR,
            mqtt.MQTT_LOG_DEBUG: logging.DEBUG
        }
        logging.log(levels.get(level, logging.DEBUG), f"MQTT Log: {buf}")

    def connect(self) -> bool:
        try:
            logging.info(f"Initiating connection to {self.config['broker']}:{self.config['port']}")
            self.client.connect(
                self.config['broker'], self.config['port'], keepalive=60)
            self.client.loop_start()

            timeout = time.time() + 10
            while not self.connection_handler.connected and time.time() < timeout:
                time.sleep(0.1)

            if not self.connection_handler.connected:
                logging.error("Connection timeout after 10 seconds")
                return False

            return True

        except Exception as e:
            logging.error(f"Connection failed: {str(e)}")
            return False

    def disconnect(self) -> None:
        logging.info("Initiating disconnect...")
        self.client.loop_stop()
        self.client.disconnect()
        logging.info("Disconnected and loop stopped")

    def publish(self, topic: str, message: str, qos: int = 1) -> bool:
        try:
            if not self.connection_handler.connected:
                logging.warning("Not connected. Attempting to reconnect...")
                if not self.connect():
                    logging.error("Reconnection failed")
                    return False

            result = self.client.publish(topic, message, qos=qos)

            if result[0] == 0:
                logging.info(f"Published successfully to {topic}")
                logging.debug(f"Message: {message}")
                return True
            else:
                logging.error(f"Failed to publish message (code: {result[0]})")
                return False

        except Exception as e:
            logging.error(f"Publishing error: {str(e)}")
            return False

    def subscribe(self, topic: str, qos: int = 1) -> bool:
        try:
            result = self.client.subscribe(topic, qos)

            if result[0] == 0:
                self.connection_handler.subscribed_topics.add(topic)
                logging.info(f"Subscribed successfully to {topic}")
                return True
            else:
                logging.error(f"Failed to subscribe to {topic} (code: {result[0]})")
                return False

        except Exception as e:
            logging.error(f"Subscription error: {str(e)}")
            return False
