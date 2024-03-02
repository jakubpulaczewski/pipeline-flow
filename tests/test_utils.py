import random
from typing import Any, Type

DEFAULT_PLUGIN_TYPES = ["s3"]
# ['jdbc', 's3', 'api', 'ftp', 'http', 'https']


def generate_plugin_name() -> str:
    """Generates a plugin name from a default list.

    Returns:
        str: A name of the plugin.
    """
    return random.choice(DEFAULT_PLUGIN_TYPES)


def create_fake_class(key: str) -> Type[Any]:
    """Creates a fake class using one of the ETL Interfaces.

    Args:
        key (str): Name of the plugin for the ETL stage.

    Returns:
        Type[Any]: A new class type with the specified key as its name.
    """
    return type(key, (object,), {})
