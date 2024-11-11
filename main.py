# main.py
import json
import time
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv
import os

from mqtt_client.config import ConfigurationManager
from mqtt_client.ssl_context import HiveMQSSLContextFactory
from mqtt_client.message_handler import DefaultMessageHandler
from mqtt_client.connection_handler import DefaultConnectionHandler
from mqtt_client.mqtt_client import MQTTClientWrapper

if __name__ == "__main__":
    try:
        # Load environment variables first
        load_dotenv()

        # Initialize configuration
        config_manager = ConfigurationManager()
        mqtt_config = config_manager.get_mqtt_config()

        # Create components
        ssl_factory = HiveMQSSLContextFactory(mqtt_config['cert'])
        message_handler = DefaultMessageHandler()
        connection_handler = DefaultConnectionHandler()

        # Create and connect client
        client = MQTTClientWrapper(
            mqtt_config,
            ssl_factory,
            message_handler,
            connection_handler
        )

        if client.connect():
            # Subscribe to test topic
            client.subscribe("test/topic")

            # Main loop with message counter
            count = 0
            while True:
                message = {
                    "count": count,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "client_id": mqtt_config['client_id']
                }
                if client.publish("test/topic", json.dumps(message)):
                    count += 1
                time.sleep(2)
        else:
            logging.error("Failed to establish connection")
            sys.exit(1)

    except KeyboardInterrupt:
        logging.info("Application terminated by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        logging.exception(e)
    finally:
        if 'client' in locals():
            client.disconnect()
        logging.info("Application shutdown complete")
