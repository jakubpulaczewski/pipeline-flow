# Standard Imports
from __future__ import annotations

import json
import logging

# Third Party Imports
import boto3
from botocore import exceptions
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Local Imports
from pipeline_flow.plugins import ISecretManager


def is_json_string(value: str) -> bool:
    try:
        parsed = json.loads(value)
        return isinstance(parsed, dict)  # True if it's a JSON object/dictionary
    except (json.JSONDecodeError, TypeError):
        return False


class AWSSecretManager(ISecretManager, plugin_name="aws_secret_manager"):
    """A class for fetching secrets from AWS Secret Manager."""

    def __init__(self, plugin_id: str, region: str, secret_name: str) -> None:
        super().__init__(plugin_id)
        self.client = boto3.client("secretsmanager", region_name=region)
        self.secret_name = secret_name

    @property
    def resource_id(self) -> str:
        """Get the ARN of the secret from AWS Secrets Manager."""
        try:
            response = self.client.describe_secret(SecretId=self.secret_name)
            arn = response["ARN"]
        except exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                msg = f"Secret {self.secret_name} not found when retrieving ARN."
                logging.error(msg)
                raise
            elif error_code == "AccessDeniedException":  # noqa: RET506
                msg = "Permission denied when retrieving ARN. Check IAM roles."
                logging.error(msg)
                raise
            else:
                logging.error("Error retrieving ARN for secret %s: %s", self.secret_name, str(e))
                raise
        else:
            logging.info("Retrieved ARN for secret %s: %s", self.secret_name, arn)
            return arn

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
        except exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "ResourceNotFoundException":
                msg = f"The requested secret {self.secret_name} was not found."
                logging.error(msg)
            elif error_code == "AccessDeniedException":
                msg = "Permission denied. Check IAM roles."
                logging.error(msg)

            raise
        else:
            secret_value = response["SecretString"]

            if is_json_string(secret_value):
                return json.loads(secret_value)

            return secret_value
