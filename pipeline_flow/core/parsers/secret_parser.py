# Standard Imports
from typing import Any, NotRequired, Self, TypedDict

# Local Imports
from pipeline_flow.core.registry import PluginRegistry
from pipeline_flow.plugins import ISecretProvider


class SecretDocument(TypedDict):
    """A type definition for the secret document."""

    plugin: str
    id: NotRequired[str]
    secret_name: str
    params: NotRequired[dict[str, Any]]


class SecretPlaceholder:
    """A class for delaying the resolution of secrets until they are needed."""

    def __init__(self: Self, secret_name: str, secret_provider: ISecretProvider) -> None:
        self.secret_name = secret_name
        self.secret_provider = secret_provider

    def resolve(self: Self) -> str:
        """Fetches the secret value by secret_name."""
        return self.secret_provider(secret_name=self.secret_name)

    def __repr__(self: Self) -> str:
        """Prevent secrets from being printed or logged."""
        return f"<SecretPlaceholder: {self.secret_name} (hidden)>"


def secret_parser(document: SecretDocument) -> dict[str, SecretPlaceholder]:
    secrets = {}

    for secret, secret_data in document.items():
        plugin_provider = PluginRegistry.instantiate_plugin(secret_data)
        secrets[secret] = SecretPlaceholder(secret_data["secret_name"], plugin_provider)

    return secrets
