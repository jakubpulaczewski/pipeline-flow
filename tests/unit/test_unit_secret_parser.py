# Standard Imports

# Third Party Imports
import pytest
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.parsers.secret_parser import SecretReference, secret_parser, secret_resolver
from pipeline_flow.core.registry import PluginRegistry
from tests.resources.plugins import SimpleSecretPlugin


@pytest.mark.parametrize(
    ("secret_identifier", "expected_secret_id", "expected_key_path"),
    [
        ("SECRET_IDENTIFIER", "SECRET_IDENTIFIER", None),
        ("SECRET_IDENTIFIER.username", "SECRET_IDENTIFIER", "username"),
    ],
)
def test_create_secret_reference(secret_identifier: str, expected_secret_id: str, expected_key_path: str) -> None:
    result = SecretReference.parse(secret_identifier)

    assert result == SecretReference(secret_id=expected_secret_id, key_path=expected_key_path)


def test_secret_parser_returns_secret_placeholder(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=SimpleSecretPlugin)

    document = {"test_secret": {"plugin": "test_plugin", "args": {"secret_name": "my-secret", "region": "aws-region"}}}
    secrets = secret_parser(document)

    assert "test_secret" in secrets
    assert isinstance(secrets["test_secret"], SimpleSecretPlugin)


def test_resolve_secret_placeholder() -> None:
    plugin = SimpleSecretPlugin("test_123", "secret_name", "region")
    secret_ref = SecretReference(secret_id="secret_id123", key_path=None)
    secret_value = secret_resolver(plugin, secret_ref)

    assert secret_value == "super_secret_value"  # noqa: S105


def test_resolve_secret_resource_id() -> None:
    plugin = SimpleSecretPlugin("test_123", "secret_name", "region")
    secret_ref = SecretReference(secret_id="secret_id123", key_path="resource_id")
    resource_id = secret_resolver(plugin, secret_ref)

    assert resource_id == "fake_arn_to_secret"
