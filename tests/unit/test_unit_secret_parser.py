# Standard Imports

# Third Party Imports
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.parsers.secret_parser import SecretPlaceholder, secret_parser
from pipeline_flow.core.registry import PluginRegistry
from tests.resources.plugins import SimpleSecretPlugin


def test_secret_parser_returns_secret_placeholder(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=SimpleSecretPlugin)

    document = {"test_secret": {"plugin": "test_plugin", "secret_name": "my-secret"}}
    secrets = secret_parser(document)

    assert "test_secret" in secrets
    assert repr(secrets["test_secret"]) == "<SecretPlaceholder: my-secret (hidden)>"


def test_resolve_secret_placeholder() -> None:
    secret_placeholder = SecretPlaceholder(
        secret_name="my-secret", secret_provider=SimpleSecretPlugin(plugin_id="test_plugin")
    )
    secret_value = secret_placeholder.resolve()

    assert secret_value == "super_secret_value"
    assert repr(secret_placeholder) == "<SecretPlaceholder: my-secret (hidden)>"
