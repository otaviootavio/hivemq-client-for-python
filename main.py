import json
import logging
from dotenv import load_dotenv
import os
import sys
from datetime import datetime
from typing import Optional, Dict

from mqtt_client.config import ConfigurationManager
from mqtt_client.ssl_context import HiveMQSSLContextFactory
from mqtt_client.message_handler import DefaultMessageHandler, Message
from mqtt_client.connection_handler import DefaultConnectionHandler
from mqtt_client.mqtt_client import MQTTClientWrapper


class DatabaseMessageHandler(DefaultMessageHandler):
    """Message handler that stores MQTT messages in PostgreSQL database"""

    def __init__(self, db_client):
        super().__init__()
        self.db_client = db_client

    def handle_message(self, message: Message) -> None:
        # Call parent method to maintain message history
        super().handle_message(message)

        try:
            # Store message in database
            # Using default False for retain flag since it's not available in the Message object
            message_id = self.db_client.create_message(
                topic=message.topic,
                payload=message.payload,
                qos=message.qos,
                retain=False,  # Default value since retain isn't available
                # Safely get client_id if it exists
                client_id=getattr(message, 'client_id', None)
            )

            # Log successful storage
            logging.info(f"Stored message {message_id} in database")

        except Exception as e:
            logging.error(f"Failed to store message in database: {e}")
            logging.debug("Message object attributes:", vars(message))


def parse_db_url(url: str) -> Dict[str, str]:
    """Parse database URL into connection parameters."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'database': parsed.path[1:],
        'user': parsed.username,
        'password': parsed.password,
        'port': parsed.port or 5432
    }


def main():
    try:
        # Load environment variables
        load_dotenv()

        # Get database configuration
        db_url = os.getenv('DB_URL')
        if not db_url:
            raise ValueError("DB_URL environment variable not set")

        # Initialize database client
        from database import PostgresMQTTClient
        db_client = PostgresMQTTClient(**parse_db_url(db_url))
        logging.info("Successfully connected to database")

        with open('cert.pem', 'r') as cert_file:
            cert_content = cert_file.read()

        # Initialize MQTT configuration
        config_manager = ConfigurationManager(
            mqtt_broker=os.getenv('MQTT_BROKER'),
            mqtt_client_id=os.getenv('MQTT_CLIENT_ID'),
            mqtt_username=os.getenv('MQTT_USERNAME'),
            mqtt_password=os.getenv('MQTT_PASSWORD'),
            hivemq_cloud_cert=cert_content,
            mqtt_port=int(os.getenv('MQTT_PORT', '8883'))
        )
        mqtt_config = config_manager.get_mqtt_config()

        # Create MQTT components
        ssl_factory = HiveMQSSLContextFactory(mqtt_config['cert'])
        message_handler = DatabaseMessageHandler(db_client)
        connection_handler = DefaultConnectionHandler()

        # Add before creating MQTTClientWrapper
        logging.debug(f"MQTT Broker: {os.getenv('MQTT_BROKER')}")
        logging.debug(f"MQTT Port: {os.getenv('MQTT_PORT')}")
        logging.debug(f"Certificate length: {len(cert_content)}")

        # Create and connect MQTT client
        mqtt_client = MQTTClientWrapper(
            mqtt_config,
            ssl_factory,
            message_handler,
            connection_handler
        )

        if mqtt_client.connect():
            print("\nConnected to MQTT broker and database successfully!")
            print("Listening for messages and storing them... (Press Ctrl+C to exit)\n")

            # Subscribe to all messages using wildcard
            mqtt_client.subscribe("#")

            # Keep the script running
            while True:
                pass

        else:
            logging.error("Failed to establish MQTT connection")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        logging.exception(e)
    finally:
        if 'mqtt_client' in locals():
            mqtt_client.disconnect()
        if 'db_client' in locals():
            db_client.close()
        print("Bridge shutdown complete")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
