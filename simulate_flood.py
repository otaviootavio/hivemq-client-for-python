import json
import os
import time
import math
import random
from datetime import datetime
import logging
from dotenv import load_dotenv
import sys

from mqtt_client.ssl_context import HiveMQSSLContextFactory
from mqtt_client.connection_handler import DefaultConnectionHandler
from mqtt_client.mqtt_client import MQTTClientWrapper
from mqtt_client.config import ConfigurationManager


class FloodLevelSimulator:
    def __init__(self, period_seconds=30, base_level=50, amplitude=30):
        """
        Initialize the flood level simulator.

        Args:
            period_seconds (int): Time for one complete sine wave cycle
            base_level (float): Base water level in centimeters
            amplitude (float): Maximum deviation from base level in centimeters
        """
        self.period_seconds = period_seconds
        self.base_level = base_level
        self.amplitude = amplitude
        self.start_time = time.time()

    def get_current_level(self) -> float:
        """Calculate current flood level based on sine wave."""
        current_time = time.time()
        elapsed_time = current_time - self.start_time

        # Calculate sine wave position (0 to 2π)
        wave_position = (2 * math.pi * elapsed_time) / self.period_seconds

        # Calculate base sine wave value (-1 to 1)
        sine_value = math.sin(wave_position)

        # Add some random noise (±5% of amplitude)
        noise = random.uniform(-0.05, 0.05) * self.amplitude

        # Calculate final level with noise
        level = self.base_level + (sine_value * self.amplitude) + noise

        return round(level, 2)

    def get_message(self) -> int:
        """Generate a message with current flood level and metadata."""
        level = self.get_current_level()
        timestamp = datetime.now().isoformat()

        # Define alert level based on water level
        alert_level = "normal"
        if level > 90:
            alert_level = "critical"
        elif level > 70:
            alert_level = "warning"
        elif level > 60:
            alert_level = "caution"

        return level


def main():
    try:
        # Load environment variables
        load_dotenv()

        # Initialize MQTT configuration
        with open('cert.pem', 'r') as cert_file:
            cert_content = cert_file.read()

        # Initialize MQTT configuration
        config_manager = ConfigurationManager(
            mqtt_broker=os.getenv('MQTT_BROKER'),
            mqtt_username=os.getenv('MQTT_USERNAME_2'),
            mqtt_password=os.getenv('MQTT_PASSWORD_2'),
            hivemq_cloud_cert=cert_content,
            mqtt_port=int(os.getenv('MQTT_PORT', '8883'))
        )
        mqtt_config = config_manager.get_mqtt_config()

        # Create MQTT components
        ssl_factory = HiveMQSSLContextFactory(mqtt_config['cert'])
        connection_handler = DefaultConnectionHandler()

        # Create and connect MQTT client
        mqtt_client = MQTTClientWrapper(
            mqtt_config,
            ssl_factory,
            None,  # No message handler needed for publishing
            connection_handler
        )

        if mqtt_client.connect():
            print("\nConnected to MQTT broker successfully!")
            print("Starting flood level simulation... (Press Ctrl+C to exit)\n")

            # Create simulator instance
            simulator = FloodLevelSimulator(
                period_seconds=30,  # 30-second period
                base_level=50,      # 50cm base level
                amplitude=30        # ±30cm variation
            )

            # Publish loop
            while True:
                # Generate message
                message = simulator.get_message()

                # Convert to JSON and publish
                mqtt_client.publish(
                    topic="sensors/flood/main_street",
                    message=message,  # Changed 'payload' to 'message'
                    qos=1
                )
                # Wait before next reading
                time.sleep(1)  # Update every second

        else:
            logging.error("Failed to establish connection")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nShutting down simulator...")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        logging.exception(e)
    finally:
        if 'mqtt_client' in locals():
            mqtt_client.disconnect()
        print("Simulator shutdown complete")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
