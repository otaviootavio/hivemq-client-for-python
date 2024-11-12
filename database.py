import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Optional, Union
from datetime import datetime

from dotenv import load_dotenv
import os
from urllib.parse import urlparse
import time
from typing import Dict


def parse_db_url(url: str) -> Dict[str, str]:
    """Parse database URL into connection parameters.

    Args:
        url (str): Database URL string

    Returns:
        dict: Connection parameters
    """
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'database': parsed.path[1:],  # Remove leading slash
        'user': parsed.username,
        'password': parsed.password,
        'port': parsed.port or 5432
    }


class PostgresMQTTClient:
    """PostgreSQL client for MQTT message storage and retrieval."""

    def __init__(self, host: str, database: str, user: str, password: str, port: int = 5432):
        """Initialize PostgreSQL connection.

        Args:
            host (str): Database host
            database (str): Database name
            user (str): Database user
            password (str): Database password
            port (int): Database port (default: 5432)
        """
        self.conn_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.connection = None
        self._connect()
        self._create_table()

    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            self.connection.autocommit = False  # Ensure explicit transaction control
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to database: {e}")

    def _create_table(self) -> None:
        """Create MQTT messages table if it doesn't exist."""
        create_table_query = """
        DROP TABLE IF EXISTS mqtt_messages;
        CREATE TABLE mqtt_messages (
            id SERIAL PRIMARY KEY,
            topic VARCHAR(255) NOT NULL,
            payload TEXT,
            qos INTEGER,
            retain BOOLEAN DEFAULT FALSE,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            client_id VARCHAR(128)
        );
        """
        try:
            with self.connection.cursor() as cursor:
                # Execute the create table query
                cursor.execute(create_table_query)
                # Explicitly commit the transaction
                self.connection.commit()
                print("Table created/reset successfully")
        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Failed to create table: {e}")

    def create_message(self, topic: str, payload: str, qos: int = 0,
                       retain: bool = False, client_id: Optional[str] = None) -> int:
        """Create a new MQTT message record."""
        if not self.connection or self.connection.closed:
            self._connect()

        query = """
        INSERT INTO mqtt_messages (topic, payload, qos, retain, client_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (topic, payload, qos, retain, client_id))
                message_id = cursor.fetchone()[0]
                self.connection.commit()
                return message_id
        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Failed to create message: {e}")

    def read_message(self, message_id: int) -> Optional[Dict]:
        """Read a specific MQTT message."""
        if not self.connection or self.connection.closed:
            self._connect()

        query = "SELECT * FROM mqtt_messages WHERE id = %s;"
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (message_id,))
                return cursor.fetchone()
        except psycopg2.Error as e:
            raise Exception(f"Failed to read message: {e}")

    def read_messages_by_topic(self, topic: str, limit: int = 100) -> List[Dict]:
        """Read messages for a specific topic."""
        if not self.connection or self.connection.closed:
            self._connect()

        query = "SELECT * FROM mqtt_messages WHERE topic = %s ORDER BY timestamp DESC LIMIT %s;"
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (topic, limit))
                return cursor.fetchall()
        except psycopg2.Error as e:
            raise Exception(f"Failed to read messages: {e}")

    def update_message(self, message_id: int,
                       payload: Optional[str] = None,
                       qos: Optional[int] = None,
                       retain: Optional[bool] = None) -> bool:
        """Update an existing MQTT message."""
        if not self.connection or self.connection.closed:
            self._connect()

        updates = []
        params = []
        if payload is not None:
            updates.append("payload = %s")
            params.append(payload)
        if qos is not None:
            updates.append("qos = %s")
            params.append(qos)
        if retain is not None:
            updates.append("retain = %s")
            params.append(retain)

        if not updates:
            return False

        query = f"""
        UPDATE mqtt_messages
        SET {', '.join(updates)}
        WHERE id = %s;
        """
        params.append(message_id)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                rows_affected = cursor.rowcount
                self.connection.commit()
                return rows_affected > 0
        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Failed to update message: {e}")

    def delete_message(self, message_id: int) -> bool:
        """Delete a specific MQTT message."""
        if not self.connection or self.connection.closed:
            self._connect()

        query = "DELETE FROM mqtt_messages WHERE id = %s;"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (message_id,))
                rows_affected = cursor.rowcount
                self.connection.commit()
                return rows_affected > 0
        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Failed to delete message: {e}")

    def delete_messages_by_topic(self, topic: str) -> int:
        """Delete all messages for a specific topic."""
        if not self.connection or self.connection.closed:
            self._connect()

        query = "DELETE FROM mqtt_messages WHERE topic = %s;"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (topic,))
                rows_affected = cursor.rowcount
                self.connection.commit()
                return rows_affected
        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Failed to delete messages: {e}")

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()


def main():
    # Load environment variables
    load_dotenv()

    # Get database URL from environment
    db_url = os.getenv('DB_URL')
    if not db_url:
        raise ValueError("DB_URL environment variable not set")

    # Parse connection parameters
    conn_params = parse_db_url(db_url)

    # Initialize client
    try:
        client = PostgresMQTTClient(**conn_params)
        print("Successfully connected to database")

        # Example operations

        # Create some test messages
        message_id1 = client.create_message(
            topic="sensors/temperature",
            payload="25.5",
            qos=1,
            retain=True,
            client_id="sensor1"
        )
        print(f"Created message with ID: {message_id1}")

        message_id2 = client.create_message(
            topic="sensors/humidity",
            payload="65",
            qos=1,
            client_id="sensor1"
        )
        print(f"Created message with ID: {message_id2}")

        # Read a specific message
        message = client.read_message(message_id1)
        print(f"Read message: {message}")

        # Read messages by topic
        temp_messages = client.read_messages_by_topic("sensors/temperature")
        print(f"Temperature messages: {temp_messages}")

        # Update a message
        updated = client.update_message(
            message_id1,
            payload="26.0",
            qos=2
        )
        print(f"Message update successful: {updated}")

        # Delete a message
        deleted = client.delete_message(message_id2)
        print(f"Message deletion successful: {deleted}")

        # Delete messages by topic
        deleted_count = client.delete_messages_by_topic("sensors/temperature")
        print(f"Deleted {deleted_count} messages from temperature topic")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'client' in locals():
            client.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()
