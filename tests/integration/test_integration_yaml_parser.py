# Standard Imports
import textwrap

# Third Party Imports
import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.parsers.yaml_parser import YamlParser
from pipeline_flow.core.registry import PluginRegistry
from tests.resources.plugins import SimpleSecretPlugin


def test_parse_secrets_in_yaml(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=SimpleSecretPlugin)
    yaml_with_secrets = textwrap.dedent("""
    secrets:
        SECRET1:
            plugin: simple_secret_plugin
            secret_name: my-secret1

    ---
    key1: ${{ secrets.SECRET1 }}
    """)

    parsed_yaml = YamlParser(stream=yaml_with_secrets).content
    assert repr(parsed_yaml["key1"]) == "<SecretPlaceholder: my-secret1 (hidden)>"


def test_parse_secrets_with_same_yaml_document() -> None:
    yaml_with_secrets = textwrap.dedent("""
    secrets:
        SECRET1:
            plugin: simple_secret_plugin
            secret_name: my-secret1

    key1: ${{ secrets.SECRET1 }}
    """)

    with pytest.raises(ValueError, match="Secret `SECRET1` is not set."):
        _ = YamlParser(stream=yaml_with_secrets).content


def test_parse_string_variables_in_yaml() -> None:
    yaml_with_vars = textwrap.dedent("""
    variables:
        VAR1: value1
        VAR2: value2

    ---
    key1: ${{ variables.VAR1 }}
    key2: ${{ variables.VAR2 }}
    """)

    parsed_yaml = YamlParser(stream=yaml_with_vars).content

    assert parsed_yaml["key1"] == "value1"
    assert parsed_yaml["key2"] == "value2"


def test_parse_int_variables_in_yaml() -> None:
    yaml_with_vars = textwrap.dedent("""
    variables:
        VAR1: 123

    ---
    key1: ${{ variables.VAR1 }}
    """)

    parsed_yaml = YamlParser(stream=yaml_with_vars).content

    assert parsed_yaml["key1"] == 123


def test_parse_dict_variables_in_yaml() -> None:
    yaml_with_vars = textwrap.dedent("""
    variables:
        DICT1:
            dict_key1: value1
            dict_key2: value2

    ---
    key1: ${{ variables.DICT1 }}
    """)

    parsed_yaml = YamlParser(stream=yaml_with_vars).content

    assert parsed_yaml["key1"] == {"dict_key1": "value1", "dict_key2": "value2"}


def test_parse_list_variables_in_yaml() -> None:
    yaml_with_vars = textwrap.dedent("""
    variables:
        LIST1:
            - value1
            - value2

    ---
    key1: ${{ variables.LIST1 }}
    """)

    parsed_yaml = YamlParser(stream=yaml_with_vars).content

    assert parsed_yaml["key1"] == ["value1", "value2"]


def test_parse_env_variables_in_yaml(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("ENV_VAR1", "VALUE_OF_ENV_1")
    monkeypatch.setenv("ENV_VAR2", "VALUE_OF_ENV_2")

    yaml_with_env_vars = """
    value1: ${{ env.ENV_VAR1 }}
    value2: ${{ env.ENV_VAR2 }}
    """

    parsed_yaml = YamlParser(stream=yaml_with_env_vars).content

    assert parsed_yaml["value1"] == "VALUE_OF_ENV_1"
    assert parsed_yaml["value2"] == "VALUE_OF_ENV_2"


def test_parse_env_variables_multiple_occurrences(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("ENV_VAR1", "VALUE_OF_ENV_1")

    yaml_with_env_vars = """
    value1: ${{ env.ENV_VAR1 }}
    value2: ${{ env.ENV_VAR1 }}
    """

    parsed_yaml = YamlParser(stream=yaml_with_env_vars).content

    assert parsed_yaml["value1"] == "VALUE_OF_ENV_1"
    assert parsed_yaml["value2"] == "VALUE_OF_ENV_1"


def test_parse_env_variables_without_env_prefix(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("ENV_VAR1", "VALUE_OF_ENV_1")

    yaml_with_env_vars = """
    value1: ${{ ENV_VAR1 }}
    """

    parsed_yaml = YamlParser(stream=yaml_with_env_vars).content

    # An environment variable without the `env.` prefix should be treated as a string
    # It does not trigger the substitution of the environment variable
    assert parsed_yaml["value1"] == "${{ ENV_VAR1 }}"
