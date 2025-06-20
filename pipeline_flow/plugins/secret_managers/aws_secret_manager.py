# Standard Imports
from __future__ import annotations

import logging

# Third Party Imports
import boto3
from botocore import exceptions
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Local Imports
from pipeline_flow.plugins import ISecretManager


class AWSSecretManager(ISecretManager, plugin_name="aws_secret_manager"):
    """A class for fetching secrets from AWS Secret Manager."""

    def __init__(self, plugin_id: str, region: str, secret_name: str) -> None:
        super().__init__(plugin_id)
        self.client = boto3.client("secretsmanager", region_name=region)
        self.secret_name = secret_name

    @retry(
        retry=retry_if_exception_type(exceptions.EndpointConnectionError),
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff (2s, 4s, 8s...)
        stop=stop_after_attempt(3),
        reraise=True,  # Raise exception if all retries fail
    )
    def __call__(self) -> str:
        """Fetches the secret value by secret_name."""

        try:
            logging.info("Fetching secret %s from AWS Secret Manager.", self.secret_name)
            response = self.client.get_secret_value(SecretId=self.secret_name)
            logging.info("Secret fetched successfully.")
            return response["SecretString"]
        except exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "ResourceNotFoundException":
                msg = f"The requested secret {self.secret_name} was not found."
                logging.error(msg)
            elif error_code == "AccessDeniedException":
                msg = "Permission denied. Check IAM roles."
                logging.error(msg)

            raise
