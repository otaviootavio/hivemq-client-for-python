import os
import logging
import sys
import time
from typing import Dict


class ConfigurationManager2:
    """Responsible for managing configuration and environment variables"""
    REQUIRED_ENV_VARS = {
        'MQTT_BROKER': 'HiveMQ Cloud broker URL',
        'MQTT_USERNAME': 'HiveMQ Cloud username',
        'MQTT_PASSWORD': 'HiveMQ Cloud password',
        'HIVEMQ_CLOUD_CERT': 'HiveMQ Cloud CA certificate'
    }

    def __init__(self):
        self._validate_env_vars()
        self.setup_logging()

    def _validate_env_vars(self) -> None:
        missing_vars = [
            f"{var} ({desc})"
            for var, desc in self.REQUIRED_ENV_VARS.items()
            if not os.getenv(var)
        ]

        if missing_vars:
            for var in missing_vars:
                logging.error(f"Missing required environment variable: {var}")
            logging.error("Please check your .env file")
            sys.exit(1)

    @staticmethod
    def setup_logging() -> None:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_mqtt_config(self) -> Dict[str, str]:
        cert = os.getenv('HIVEMQ_CLOUD_CERT', "")
        logging.debug(f"Certificate from env starts with: {cert[:50]}")

        return {
            'broker': os.getenv('MQTT_BROKER'),
            'port': int(os.getenv('MQTT_PORT', '8883')),
            'username': os.getenv('MQTT_USERNAME_2'),
            'password': os.getenv('MQTT_PASSWORD_2'),
            'cert': cert,
            'client_id': os.getenv('MQTT_CLIENT_ID_2', f'python_client_{int(time.time())}')
        }
