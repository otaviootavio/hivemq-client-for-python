import json
import logging
from dotenv import load_dotenv
import os
import sys
from datetime import datetime

from mqtt_client.config import ConfigurationManager
from mqtt_client.ssl_context import HiveMQSSLContextFactory
from mqtt_client.message_handler import DefaultMessageHandler, Message
from mqtt_client.connection_handler import DefaultConnectionHandler
from mqtt_client.mqtt_client import MQTTClientWrapper


class EchoMessageHandler(DefaultMessageHandler):
    """Message handler that echoes messages to console"""

    def handle_message(self, message: Message) -> None:
        # Call parent method to maintain message history
        super().handle_message(message)

        # Pretty print the message
        try:
            # Try to parse as JSON for prettier output
            payload = json.loads(message.payload)
            payload_str = json.dumps(payload, indent=2)
        except json.JSONDecodeError:
            # If not JSON, use raw payload
            payload_str = message.payload

        print("\n=== New Message ===")
        print(f"Timestamp: {message.timestamp}")
        print(f"Topic: {message.topic}")
        print(f"QoS: {message.qos}")
        print("Payload:")
        print(payload_str)
        print("=================\n")


def main():
    try:
        # Load environment variables
        load_dotenv()

        with open('cert.pem', 'r') as cert_file:
            cert_content = cert_file.read()

        # Initialize configuration
        config_manager = config_manager = ConfigurationManager(
            mqtt_broker=os.getenv('MQTT_BROKER'),
            mqtt_username=os.getenv('MQTT_USERNAME_2'),
            mqtt_password=os.getenv('MQTT_PASSWORD_2'),
            hivemq_cloud_cert=cert_content,
            mqtt_port=int(os.getenv('MQTT_PORT', '8883'))
        )
        mqtt_config = config_manager.get_mqtt_config()

        # Create components
        ssl_factory = HiveMQSSLContextFactory(mqtt_config['cert'])
        message_handler = EchoMessageHandler()
        connection_handler = DefaultConnectionHandler()

        # Create and connect client
        client = MQTTClientWrapper(
            mqtt_config,
            ssl_factory,
            message_handler,
            connection_handler
        )

        if client.connect():
            print("\nConnected to MQTT broker successfully!")
            print("Listening for messages... (Press Ctrl+C to exit)\n")

            # Subscribe to all messages using wildcard
            client.subscribe("#")  # Subscribe to all topics

            # Keep the script running
            while True:
                pass

        else:
            logging.error("Failed to establish connection")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        logging.exception(e)
    finally:
        if 'client' in locals():
            client.disconnect()
        print("Echo client shutdown complete")


if __name__ == "__main__":
    main()
