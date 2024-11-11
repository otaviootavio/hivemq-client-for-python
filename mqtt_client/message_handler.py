from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any
import logging


@dataclass
class Message:
    """Data class for MQTT messages"""
    topic: str
    payload: str
    timestamp: str
    qos: int


class MessageHandler(ABC):
    """Abstract base class for message handling"""
    @abstractmethod
    def handle_message(self, message: Message) -> None:
        pass


class DefaultMessageHandler(MessageHandler):
    """Default implementation of message handling"""

    def __init__(self):
        self.message_history: List[Message] = []

    def handle_message(self, message: Message) -> None:
        self.message_history.append(message)
        logging.info(
            f"Message received - Topic: {message.topic}, QoS: {message.qos}")
        logging.debug(f"Message payload: {message.payload}")
