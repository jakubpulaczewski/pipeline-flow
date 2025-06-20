# Standard Imports
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic.dataclasses import dataclass

# Local Imports
from pipeline_flow.core.registry import PluginRegistry

if TYPE_CHECKING:
    from pipeline_flow.common.type_def import PluginPayload
    from pipeline_flow.plugins import ISecretManager


@dataclass
class SecretReference:
    """Represents a parsed secret reference with optional nested path."""

    secret_id: str
    key_path: str | None = None

    @classmethod
    def parse(cls, secret_expression: str) -> SecretReference:
        """Parse a secret expression like 'SECRET1.username' into components."""
        parts = secret_expression.split(".", 1)
        secret_id = parts[0]
        key_path = parts[1] if len(parts) > 1 else None
        return cls(secret_id=secret_id, key_path=key_path)


def secret_resolver(secret_provider: ISecretManager, secret_ref: SecretReference) -> str:
    """Fetches the secret value by secret_name."""
    try:
        reference = secret_provider()
    except Exception as e:
        error_msg = f"Failed to retrieve secret '{secret_ref.secret_id}': {e}"
        logging.error(error_msg)
        raise ValueError(error_msg) from e

    if secret_ref.key_path is None:
        return reference

    try:
        return reference[secret_ref.key_path]

    except KeyError as e:
        available_keys = list(reference.keys())
        error_msg = (
            f"Key path '{secret_ref.key_path}' does not exist in secret '{secret_ref.secret_id}'."
            f"Available keys: {available_keys}"
        )

        logging.error(error_msg)
        raise KeyError(error_msg) from e

    except (TypeError, AttributeError) as e:
        error_msg = (
            f"Cannot access key path '{secret_ref.key_path}' in secret '{secret_ref.secret_id}': "
            f"secret is not a dictionary or nested structure. Secret type: {type(reference)}"
        )
        logging.error(error_msg)
        raise ValueError(error_msg) from e


def secret_parser(document: PluginPayload) -> dict[str, ISecretManager]:
    secrets = {}

    for secret_id, secret_data in document.items():
        secrets[secret_id] = PluginRegistry.instantiate_plugin(secret_data)

    return secrets
