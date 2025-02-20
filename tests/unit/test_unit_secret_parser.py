# Standard Imports
from typing import Self

# Third Party Imports
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.parsers.secret_parser import SecretPlaceholder, secret_parser
from pipeline_flow.core.registry import PluginRegistry
from pipeline_flow.plugins import ISecretManager


class SimpleSecretPlugin(ISecretManager, plugin_name="simple_secret_plugin"):
    def __call__(self: Self, secret_name: str) -> str:  # noqa: ARG002 - This is a dummy implementation
        return "super_secret_value"


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
