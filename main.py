from queue import Queue, Empty
import signal
import json
import logging
from dotenv import load_dotenv
import os
import sys
from datetime import datetime
import threading
from typing import Optional, Dict

from mqtt_client.config import ConfigurationManager
from mqtt_client.ssl_context import HiveMQSSLContextFactory
from mqtt_client.message_handler import DefaultMessageHandler, Message
from mqtt_client.connection_handler import DefaultConnectionHandler
from mqtt_client.mqtt_client import MQTTClientWrapper


class OptimizedMessageHandler(DefaultMessageHandler):
    """Message handler with memory management and async processing"""

    def __init__(self, db_client, max_queue_size: int = 1000):
        super().__init__()
        self.db_client = db_client
        self.message_queue = Queue(maxsize=max_queue_size)
        self.running = True

        # Start processing thread
        self.worker = threading.Thread(target=self._process_messages)
        self.worker.daemon = True
        self.worker.start()

        # Message tracking
        self.processed_count = 0
        self.dropped_count = 0
        self.last_log_time = datetime.now()

    def handle_message(self, message: Message) -> None:
        try:
            if not self.message_queue.full():
                self.message_queue.put_nowait(message)
            else:
                self.dropped_count += 1
                if self.dropped_count % 100 == 0:
                    logging.warning(
                        f"Dropped {self.dropped_count} messages - queue full")

        except Exception as e:
            logging.error(f"Error queuing message: {e}")

    def _process_messages(self):
        """Background thread for message processing"""
        while self.running:
            try:
                # Get message with timeout to allow checking running flag
                message = self.message_queue.get(timeout=1.0)

                # Store in database
                self._store_message(message)
                self.processed_count += 1

                # Log statistics periodically
                current_time = datetime.now()
                if (current_time - self.last_log_time).total_seconds() >= 60:
                    self._log_statistics()
                    self.last_log_time = current_time

            except Empty:  # Use the imported Empty exception
                continue  # No messages to process
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    def _store_message(self, message: Message):
        """Store single message with retry logic"""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.db_client.create_message(
                    topic=message.topic,
                    payload=message.payload,
                    qos=message.qos,
                    retain=False,
                    client_id=getattr(message, 'client_id', None)
                )
                return
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    logging.error(
                        f"Failed to store message after {max_retries} attempts")
                    raise
                logging.warning(f"Retry {retry_count} for message storage")

    def _log_statistics(self):
        """Log processing statistics"""
        logging.info(
            f"Messages - Processed: {self.processed_count}, "
            f"Queued: {self.message_queue.qsize()}, "
            f"Dropped: {self.dropped_count}"
        )

    def shutdown(self):
        """Graceful shutdown of message processing"""
        self.running = False
        self.worker.join(timeout=5.0)

        # Process remaining messages
        remaining = self.message_queue.qsize()
        if remaining > 0:
            logging.info(f"Processing {remaining} remaining messages...")
            while not self.message_queue.empty():
                try:
                    message = self.message_queue.get_nowait()
                    self._store_message(message)
                except Exception as e:
                    logging.error(f"Error processing final message: {e}")


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
        message_handler = OptimizedMessageHandler(
            db_client, max_queue_size=1000)
        connection_handler = DefaultConnectionHandler()

        # Create and connect MQTT client
        mqtt_client = MQTTClientWrapper(
            mqtt_config,
            ssl_factory,
            message_handler,
            connection_handler
        )

        def handle_shutdown(signum, frame):
            """Handle graceful shutdown"""
            print("\nShutting down...")
            message_handler.shutdown()
            mqtt_client.disconnect()
            db_client.close()
            print("Bridge shutdown complete")
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        if mqtt_client.connect():
            print("\nConnected to MQTT broker and database successfully!")
            print("Listening for messages and storing them... (Press Ctrl+C to exit)\n")

            mqtt_client.subscribe("#")

            signal.pause()
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
