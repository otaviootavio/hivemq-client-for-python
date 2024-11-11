from dotenv import load_dotenv
import os
import ssl
import paho.mqtt.client as mqtt
import time
import json
from datetime import datetime
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables with error checking
load_dotenv()

# HiveMQ Cloud CA Certificate


# Required environment variables
REQUIRED_ENV_VARS = {
    'MQTT_BROKER': 'HiveMQ Cloud broker URL',
    'MQTT_USERNAME': 'HiveMQ Cloud username',
    'MQTT_PASSWORD': 'HiveMQ Cloud password',
    "HIVEMQ_CLOUD_CERT": 'HiveMQ Cloud CA certificate'
}

# Validate environment variables
missing_vars = []
for var, description in REQUIRED_ENV_VARS.items():
    if not os.getenv(var):
        missing_vars.append(f"{var} ({description})")

if missing_vars:
    logger.error("Missing required environment variables:")
    for var in missing_vars:
        logger.error(f"- {var}")
    logger.error("Please check your .env file")
    sys.exit(1)

class HiveMQClient:
    def __init__(self):
        """Initialize MQTT client with detailed configuration logging"""
        # Load configuration with validation
        self.broker = os.getenv('MQTT_BROKER')
        self.port = int(os.getenv('MQTT_PORT', '8883'))  # Force SSL port
        self.username = os.getenv('MQTT_USERNAME')
        self.password = os.getenv('MQTT_PASSWORD')
        self.hive_mq_cloud_cert = os.getenv('HIVEMQ_CLOUD_CERT')
        self.client_id = os.getenv('MQTT_CLIENT_ID', f'python_client_{int(time.time())}')
        
        # Configuration logging
        logger.info("=== MQTT Configuration ===")
        logger.info(f"Broker: {self.broker}")
        logger.info(f"Port: {self.port}")
        logger.info(f"Username: {self.username}")
        logger.info(f"Client ID: {self.client_id}")
        logger.info(f"SSL/TLS: Enabled")
        logger.info("========================")
        
        # Force SSL port if not set correctly
        if self.port != 8883:
            logger.warning(f"Port {self.port} is not secure. Forcing port 8883 for SSL/TLS")
            self.port = 8883
        
        # Create MQTT client instance
        logger.debug("Creating MQTT client instance...")
        self.client = mqtt.Client(
            client_id=self.client_id,
            protocol=mqtt.MQTTv5,
            transport="tcp"
        )
        
        # Setup SSL with debug information
        logger.debug("Configuring SSL context...")
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.load_verify_locations(cadata=self.hive_mq_cloud_cert)
            self.client.tls_set_context(ssl_context)
            logger.info("SSL context configured successfully")
        except Exception as e:
            logger.error(f"SSL configuration failed: {str(e)}")
            raise
        
        # Set authentication
        if not self.username or not self.password:
            logger.error("Missing credentials! Both username and password are required")
            raise ValueError("Missing MQTT credentials")
        
        logger.debug("Setting up authentication...")
        self.client.username_pw_set(self.username, self.password)
        logger.info("Authentication configured")
        
        # Set callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log
        
        # Initialize state
        self.connected = False
        self.message_history = []
        
        logger.info("Client initialization completed")

    def on_log(self, client, userdata, level, buf):
        """Log MQTT internal events with appropriate level"""
        levels = {
            mqtt.MQTT_LOG_INFO: logging.INFO,
            mqtt.MQTT_LOG_NOTICE: logging.INFO,
            mqtt.MQTT_LOG_WARNING: logging.WARNING,
            mqtt.MQTT_LOG_ERR: logging.ERROR,
            mqtt.MQTT_LOG_DEBUG: logging.DEBUG
        }
        logger.log(levels.get(level, logging.DEBUG), f"MQTT Log: {buf}")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        """Handle connection result with detailed error information"""
        connection_codes = {
            0: "Connection successful",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized"
        }
        
        if rc == 0:
            self.connected = True
            logger.info(f"Connected to HiveMQ Cloud: {connection_codes.get(rc)}")
            if hasattr(self, 'subscribed_topics'):
                for topic in self.subscribed_topics:
                    logger.info(f"Resubscribing to topic: {topic}")
                    self.subscribe(topic)
        else:
            self.connected = False
            error_msg = connection_codes.get(rc, f"Unknown error code: {rc}")
            logger.error(f"Connection failed: {error_msg}")
            if rc == 4:
                logger.error("Please check your username and password")
            elif rc == 5:
                logger.error("Please check your client credentials and permissions")

    def on_message(self, client, userdata, msg):
        """Process received messages with error handling"""
        try:
            payload = msg.payload.decode()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message_data = {
                "topic": msg.topic,
                "payload": payload,
                "timestamp": timestamp,
                "qos": msg.qos
            }
            self.message_history.append(message_data)
            logger.info(f"Message received - Topic: {msg.topic}, QoS: {msg.qos}")
            logger.debug(f"Message payload: {payload}")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def on_disconnect(self, client, userdata, rc, properties=None):
        """Handle disconnection with reason"""
        self.connected = False
        if rc != 0:
            logger.warning(f"Unexpected disconnection (code: {rc})")
        logger.info("Disconnected from HiveMQ Cloud")

    def connect(self):
        """Establish connection with timeout and error handling"""
        try:
            logger.info(f"Initiating connection to {self.broker}:{self.port}")
            
            # Connect with timeout
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()
            
            # Wait for connection
            timeout = time.time() + 10
            while not self.connected and time.time() < timeout:
                logger.debug("Waiting for connection...")
                time.sleep(0.1)
            
            if not self.connected:
                logger.error("Connection timeout after 10 seconds")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            if "Network is unreachable" in str(e):
                logger.error("Network connectivity issue - Check your internet connection")
            elif "Connection refused" in str(e):
                logger.error("Connection refused - Check if broker is accessible and port is correct")
            elif "certificate verify failed" in str(e):
                logger.error("SSL/TLS verification failed - Certificate issue")
            return False

    def disconnect(self):
        """Clean disconnect"""
        logger.info("Initiating disconnect...")
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Disconnected and loop stopped")

    def publish(self, topic, message, qos=1):
        """Publish message with delivery tracking"""
        try:
            if not self.connected:
                logger.warning("Not connected. Attempting to reconnect...")
                if not self.connect():
                    logger.error("Reconnection failed")
                    return False
            
            logger.debug(f"Publishing to {topic} with QoS {qos}")
            result = self.client.publish(topic, message, qos=qos)
            
            if result[0] == 0:
                logger.info(f"Published successfully to {topic}")
                logger.debug(f"Message: {message}")
                return True
            else:
                logger.error(f"Failed to publish message (code: {result[0]})")
                return False
                
        except Exception as e:
            logger.error(f"Publishing error: {str(e)}")
            return False

    
    def subscribe(self, topic, qos=1):
        """Subscribe to topic with error handling"""
        try:
            if not hasattr(self, 'subscribed_topics'):
                self.subscribed_topics = set()
            
            logger.debug(f"Subscribing to {topic} with QoS {qos}")
            result = self.client.subscribe(topic, qos)
            
            if result[0] == 0:
                self.subscribed_topics.add(topic)
                logger.info(f"Subscribed successfully to {topic}")
                return True
            else:
                logger.error(f"Failed to subscribe to {topic} (code: {result[0]})")
                return False
                
        except Exception as e:
            logger.error(f"Subscription error: {str(e)}")
            return False

if __name__ == "__main__":
    try:
        # Create client instance
        logger.info("Starting HiveMQ MQTT Client...")
        
        # Test environment variables first
        if not os.getenv('MQTT_USERNAME') or not os.getenv('MQTT_PASSWORD'):
            logger.error("Missing MQTT credentials in .env file")
            sys.exit(1)
            
        client = HiveMQClient()
        
        if client.connect():
            # Subscribe to test topic
            client.subscribe("test/topic")
            
            # Main loop with message counter
            count = 0
            while True:
                message = {
                    "count": count,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "client_id": client.client_id
                }
                if client.publish("test/topic", json.dumps(message)):
                    count += 1
                time.sleep(2)
        else:
            logger.error("Failed to establish connection")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.exception(e)  # This will print the full traceback
    finally:
        if 'client' in locals():
            client.disconnect()
        logger.info("Application shutdown complete")