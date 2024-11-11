# MQTT Client

A SOLID-based MQTT client implementation focusing on clean architecture and maintainability. Built for HiveMQ Cloud but adaptable for other MQTT brokers.

## Features

- SSL/TLS secure communication
- Message publishing and subscription
- Configurable connection settings
- Logging system for debugging
- Error handling and reconnection logic

## Quick Start

1. Install dependencies:

```bash
pip install paho-mqtt python-dotenv
```

2. Create a `.env` file with your MQTT configuration:

```
MQTT_BROKER=your-broker.hivemq.cloud
MQTT_PORT=8883
MQTT_USERNAME=your-username
MQTT_PASSWORD=your-password
MQTT_CLIENT_ID=your-client-id
HIVEMQ_CLOUD_CERT="-----BEGIN CERTIFICATE----- MIIFazCCA1OgAwIBAgIR... -----END CERTIFICATE-----"
```

3. Run the client:

```bash
python main.py
```

## Project Structure

```
mqtt_client/
├── __init__.py          # Package initialization
├── config.py            # Configuration management
├── connection_handler.py # Connection handling
├── message_handler.py   # Message processing
├── mqtt_client.py       # Main MQTT client wrapper
└── ssl_context.py       # SSL/TLS configuration
```

## Contributing

Feel free to open issues and pull requests.

## License

MIT
