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

        # More robust certificate cleaning
        cert = ca_cert.strip()

        # Check if certificate has proper BEGIN/END markers
        if "BEGIN CERTIFICATE" not in cert:
            cert = "-----BEGIN CERTIFICATE-----\n" + cert
        if "END CERTIFICATE" not in cert:
            cert += "\n-----END CERTIFICATE-----"

        # Ensure proper line breaks
        cert_lines = cert.split('\n')
        formatted_lines = []
        for line in cert_lines:
            line = line.strip()
            if line and not line.startswith('-----'):
                # Ensure each line is of proper length (64 characters)
                while line:
                    formatted_lines.append(line[:64])
                    line = line[64:]
            else:
                formatted_lines.append(line)

        self.ca_cert = '\n'.join(formatted_lines)
        logging.debug(f"Formatted certificate:\n{self.ca_cert}")

    def create_ssl_context(self) -> ssl.SSLContext:
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED

            # Try loading the certificate
            try:
                ssl_context.load_verify_locations(cadata=self.ca_cert)
            except ssl.SSLError as e:
                logging.error(f"Failed to load certificate: {e}")
                logging.debug(f"Certificate content:\n{self.ca_cert}")
                raise

            # Enable TLS 1.2 explicitly
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3

            logging.info("SSL context created successfully")
            return ssl_context
        except Exception as e:
            logging.error(f"SSL Error: {str(e)}")
            raise
