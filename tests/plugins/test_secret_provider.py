# Standard Imports
import json
from typing import Generator

# Third Party Imports
import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
from moto import mock_aws
from mypy_boto3_secretsmanager import SecretsManagerClient
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.plugins.secret_managers import AWSSecretManager


@pytest.fixture(autouse=True)
def mock_secretmanager() -> Generator[SecretsManagerClient, None, None]:
    with mock_aws():
        # Setup the environment for the mock.
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="my-secret-name", SecretString="mock-secret-value")
        client.create_secret(Name="json-secret", SecretString=json.dumps({"username": "user"}))

        yield client


def test_fetch_secret_str_success() -> None:
    # Call function using mocked AWS client
    secret_value = AWSSecretManager(plugin_id="plugin_id", secret_name="my-secret-name", region="us-east-1")()

    assert secret_value == "mock-secret-value"  # noqa: S105


def test_fetch_secret_str_json_success() -> None:
    # Call function using mocked AWS client
    secret_value = AWSSecretManager(plugin_id="plugin_id", secret_name="json-secret", region="us-east-1")()

    assert secret_value == {"username": "user"}


def test_secret_not_found(mocker: MockerFixture) -> None:
    mocker.patch("time.sleep")
    boto3_mock = mocker.patch(
        "boto3.client",
    ).return_value

    # Add some data to EndpointConnectionError to simulate a connection error.
    boto3_mock.get_secret_value.side_effect = EndpointConnectionError(
        endpoint_url="https://secretsmanager.us-east-1.amazonaws.com",
        error_message="Could not connect to the endpoint URL",
    )

    with pytest.raises(EndpointConnectionError):
        # ClientError encapsulates `ResourceNotFoundException` when secret does not exist.
        AWSSecretManager(plugin_id="plugin_id", secret_name="nonexistent-secret", region="us-east-1")()

    assert boto3_mock.get_secret_value.call_count == 3
