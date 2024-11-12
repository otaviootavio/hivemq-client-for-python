import logging
import sys
from typing import Dict, Optional


class ConfigurationManager:
    """Responsible for managing configuration and environment variables"""
    REQUIRED_ENV_VARS = {
        'MQTT_BROKER': 'HiveMQ Cloud broker URL',
        'MQTT_USERNAME': 'HiveMQ Cloud username',
        'MQTT_PASSWORD': 'HiveMQ Cloud password',
        'HIVEMQ_CLOUD_CERT': 'HiveMQ Cloud CA certificate'
    }

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
        self.mqtt_broker = mqtt_broker
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.hivemq_cloud_cert = hivemq_cloud_cert
        self.mqtt_port = mqtt_port
        self.mqtt_client_id = mqtt_client_id
        self.logging_level = logging_level

        self._validate_required_vars()
        self.setup_logging()

    def _validate_required_vars(self) -> None:
        missing_vars = []
        required_vars = {
            'MQTT_BROKER': self.mqtt_broker,
            'MQTT_USERNAME': self.mqtt_username,
            'MQTT_PASSWORD': self.mqtt_password,
            'HIVEMQ_CLOUD_CERT': self.hivemq_cloud_cert
        }

        for var, value in required_vars.items():
            if not value:
                missing_vars.append(f"{var} ({self.REQUIRED_ENV_VARS[var]})")

        if missing_vars:
            for var in missing_vars:
                logging.error(f"Missing required variable: {var}")
            logging.error("Please check your configuration")
            sys.exit(1)

    def setup_logging(self) -> None:
        logging.basicConfig(
            level=self.logging_level,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_mqtt_config(self) -> Dict[str, str]:
        logging.debug(f"Certificate starts with: {
                      self.hivemq_cloud_cert[:50]}")
        return {
            'broker': self.mqtt_broker,
            'port': self.mqtt_port,
            'username': self.mqtt_username,
            'password': self.mqtt_password,
            'cert': self.hivemq_cloud_cert,
            'client_id': self.mqtt_client_id
        }
