# Standard Imports
import logging
from abc import ABC, abstractmethod

# Third Party Imports
import boto3
from botocore import exceptions
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Local Imports


class SecretProvider(ABC):
    """A base class for providing authentication secrets."""

    @abstractmethod
    def fetch_secret(self, secret_name: str) -> str:
        """Fetches the secret value by name."""
        raise NotImplementedError("Subclasses must implement this method.")


class AWSSecretManager(SecretProvider):
    """A class for fetching secrets from AWS Secret Manager."""

    def __init__(self, region_name: str):
        self.client = boto3.client("secretsmanager", region_name=region_name)

    @retry(
        retry=retry_if_exception_type(exceptions.EndpointConnectionError),
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff (2s, 4s, 8s...)
        stop=stop_after_attempt(3),
        reraise=True,  # Raise exception if all retries fail
    )
    def fetch_secret(self, secret_name: str) -> str:
        """Fetches the secret value by name."""

        try:
            logging.info("Fetching secret %s from AWS Secret Manager.", secret_name)
            response = self.client.get_secret_value(SecretId=secret_name)
            logging.info("Secret fetched successfully.")
            return response["SecretString"]
        except exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "ResourceNotFoundException":
                msg = f"The requested secret {secret_name} was not found."
                logging.error(msg)
            elif error_code == "AccessDeniedException":
                msg = "Permission denied. Check IAM roles."
                logging.error(msg)

            raise
