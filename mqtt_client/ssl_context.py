# mqtt_client/ssl_context.py
import ssl
import logging
from abc import ABC, abstractmethod
import re


class SSLContextFactory(ABC):
    """Abstract factory for SSL context creation"""
    @abstractmethod
    def create_ssl_context(self) -> ssl.SSLContext:
        pass


class HiveMQSSLContextFactory(SSLContextFactory):
    """Concrete factory for HiveMQ SSL context"""

    def __init__(self, ca_cert: str):
        if not ca_cert:
            raise ValueError("CA certificate is empty")

        # Clean the certificate string
        cert = ca_cert.strip().strip('"').strip("'")

        # Split into lines and clean each line
        cert_parts = cert.split()

        # Reconstruct certificate with proper line breaks
        formatted_cert = "-----BEGIN CERTIFICATE-----\n"
        # Skip the BEGIN and END parts from original
        formatted_cert += "\n".join(cert_parts[2:-1])
        formatted_cert += "\n-----END CERTIFICATE-----"

        self.ca_cert = formatted_cert

    def create_ssl_context(self) -> ssl.SSLContext:
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.load_verify_locations(cadata=self.ca_cert)
            logging.info("SSL context created successfully")
            return ssl_context
        except ssl.SSLError as e:
            logging.error(f"SSL Error: {str(e)}")
            logging.debug(f"Final certificate content:\n{self.ca_cert}")
            raise
