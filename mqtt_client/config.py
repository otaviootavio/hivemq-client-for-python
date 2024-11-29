import logging
import sys
from typing import Dict, Optional


class ConfigurationManager:
    """Responsible for managing configuration and environment variables"""

    def __init__(
        self,
        mqtt_broker: str,
        mqtt_username: str,
        mqtt_password: str,
        hivemq_cloud_cert: str,
        mqtt_port: int = 8883,
        mqtt_client_id: Optional[str] = None,
        logging_level: int = logging.DEBUG
    ):
        # Strip any whitespace from credentials
        self.mqtt_broker = mqtt_broker.strip() if mqtt_broker else None
        self.mqtt_username = mqtt_username.strip() if mqtt_username else None
        self.mqtt_password = mqtt_password.strip() if mqtt_password else None
        self.hivemq_cloud_cert = hivemq_cloud_cert
        self.mqtt_port = mqtt_port
        self.mqtt_client_id = mqtt_client_id
        self.logging_level = logging_level

        self._validate_configuration()
        self.setup_logging()

    def _validate_configuration(self) -> None:
        errors = []

        if not self.mqtt_broker:
            errors.append("MQTT_BROKER is empty or not set")
        if not self.mqtt_username:
            errors.append("MQTT_USERNAME is empty or not set")
        if not self.mqtt_password:
            errors.append("MQTT_PASSWORD is empty or not set")
        if not self.hivemq_cloud_cert:
            errors.append("HIVEMQ_CLOUD_CERT is empty or not set")

        # Validate broker URL format
        if self.mqtt_broker and not self.mqtt_broker.endswith('.hivemq.cloud'):
            errors.append(
                f"Invalid HiveMQ broker URL format: {self.mqtt_broker}")

        # Validate port
        if not isinstance(self.mqtt_port, int) or self.mqtt_port <= 0:
            errors.append(f"Invalid MQTT port: {self.mqtt_port}")

        if errors:
            for error in errors:
                logging.error(f"Configuration error: {error}")
            raise ValueError("Invalid configuration. Check logs for details.")

        # Log successful validation
        logging.info("Configuration validation successful")
        logging.debug(f"Using broker: {self.mqtt_broker}")
        logging.debug(f"Using port: {self.mqtt_port}")
        logging.debug(
            f"Username length: {len(self.mqtt_username) if self.mqtt_username else 0}")
        logging.debug(
            f"Password length: {len(self.mqtt_password) if self.mqtt_password else 0}")

    def setup_logging(self) -> None:
        logging.basicConfig(
            level=self.logging_level,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_mqtt_config(self) -> Dict[str, str]:
        logging.debug(
            f"Certificate starts with: {self.hivemq_cloud_cert[:50]}")
        return {
            'broker': self.mqtt_broker,
            'port': self.mqtt_port,
            'username': self.mqtt_username,
            'password': self.mqtt_password,
            'cert': self.hivemq_cloud_cert,
            'client_id': self.mqtt_client_id
        }
