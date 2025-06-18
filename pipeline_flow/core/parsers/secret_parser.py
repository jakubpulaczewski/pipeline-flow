# Standard Imports
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, Any, Self

from pydantic.dataclasses import dataclass

# Local Imports
from pipeline_flow.core.registry import PluginRegistry

if TYPE_CHECKING:
    from pipeline_flow.plugins import ISecretManager


@dataclass
class SecretReference:
    """Represents a parsed secret reference with optional nested path."""

    secret_name: str
    key_path: str | None = None

    @classmethod
    def parse(cls, secret_expression: str) -> SecretReference:
        """Parse a secret expression like 'SECRET1.username' into components."""
        parts = secret_expression.split(".", 1)
        secret_name = parts[0]
        key_path = parts[1] if len(parts) > 1 else None
        return cls(secret_name=secret_name, key_path=key_path)


@dataclass
class SecretDocument:
    """A data class definition for the secret document."""

    plugin: Annotated[str, "Plugin Name associated with Secret Plugin"]
    secret_name: Annotated[str, "The name of the secret"]
    id: str | None = None
    params: dict[str, Any] | None = None


class SecretPlaceholder:
    """A class for delaying the resolution of secrets until they are needed."""

    def __init__(self: Self, secret_name: str, secret_provider: ISecretManager) -> None:
        self.secret_name = secret_name
        self.secret_provider = secret_provider

    def resolve(self: Self, key_path: str | None = None) -> str:
        """Fetches the secret value by secret_name."""
        try:
            reference = self.secret_provider(secret_name=self.secret_name)
        except Exception as e:
            error_msg = f"Failed to retrieve secret '{self.secret_name}': {e}"
            logging.error(error_msg)
            raise ValueError(error_msg) from e

        if key_path is None:
            return reference

        try:
            return reference[key_path]

        except KeyError as e:
            available_keys = list(reference.keys())
            error_msg = (
                f"Key path '{key_path}' does not exist in secret '{self.secret_name}'. Available keys: {available_keys}"
            )
            logging.error(error_msg)
            raise KeyError(error_msg) from e

        except (TypeError, AttributeError) as e:
            error_msg = (
                f"Cannot access key path '{key_path}' in secret '{self.secret_name}': "
                f"secret is not a dictionary or nested structure. Secret type: {type(reference)}"
            )
            logging.error(error_msg)
            raise ValueError(error_msg) from e

    def __repr__(self: Self) -> str:
        """Prevent secrets from being printed or logged."""
        return f"<SecretPlaceholder: {self.secret_name} (hidden)>"


def secret_parser(document: SecretDocument) -> dict[str, SecretPlaceholder]:
    """Parse secrets config into SecretPlaceHolder objects.

    Args:
        document (SecretDocument): A dictionary of secret plugin config.

    Returns:
        dict[str, SecretPlaceholder]: A dict mapping secret names to secret placeholder objects
    """
    secrets = {}

    for secret_name, secret_data in document.items():
        try:
            # Parse it into a secret document and validate its structure.
            secret_document = SecretDocument(**secret_data)

            secrets[secret_name] = SecretPlaceholder(
                secret_document.secret_name,
                PluginRegistry.instantiate_plugin(secret_data),
            )

        except (TypeError, ValueError) as e:
            msg = f"Invalid secret configuration for '{secret_name}': {e}"
            raise ValueError(msg)  # noqa: B904

    return secrets
