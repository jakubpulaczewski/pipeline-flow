# Standard Imports
from pathlib import Path
from unittest.mock import Mock

# Third Party Imports
import pytest
import yaml

# Project Imports
from pipeline_flow.core.parsers.yaml_parser import ExtendedCoreLoader, YamlParser
from tests.resources.plugins import SimpleSecretPlugin


@pytest.fixture(scope="session")
def yaml_pipeline() -> str:
    return """
    pipelines:
      pipeline1:
        type: ETL
        phases:
          extract:
            steps:
              - id: mock_extract1
                plugin: mock_s3
          transform:
            steps:
              - id: mock_tranformation1
                plugin: aggregate_sum
          load:
            steps:
              - id: mock_load1
                plugin: mock_s3
    """


def test_parse_invalid_yaml() -> None:
    yaml_content = """
    key1: value1
    key2 value2  # Missing colon
    key3:
    - item1
    - item2:
    """

    with pytest.raises(yaml.YAMLError):
        YamlParser(stream=yaml_content)


def test_env_var_not_defined() -> None:
    yaml_with_env_vars = """
    value1: ${{ env.ENV_VAR1 }}
    """
    with pytest.raises(ValueError, match="Environment variable `ENV_VAR1` is not set."):
        YamlParser(stream=yaml_with_env_vars)


def test_parse_variable_placeholder_string_success() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.variables = {"VAR1": "VALUE_OF_VAR1"}

    result = yaml_parser.substitute_variable_placeholder(node=Mock(value="${{ variables.VAR1 }}"))

    assert result == "VALUE_OF_VAR1"


def test_parse_variable_placeholder_int_success() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.variables = {"VAR1": 123}

    result = yaml_parser.substitute_variable_placeholder(node=Mock(value="${{ variables.VAR1 }}"))

    assert result == 123


def test_parse_variable_placeholder_list_success() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.variables = {"VAR1": [1, 2, 3]}

    result = yaml_parser.substitute_variable_placeholder(node=Mock(value="${{ variables.VAR1 }}"))

    assert result == [1, 2, 3]


def test_parse_variable_placeholder_dict_success() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.variables = {"VAR1": {"key1": "value1", "key2": "value2"}}

    result = yaml_parser.substitute_variable_placeholder(node=Mock(value="${{ variables.VAR1 }}"))

    assert result == {"key1": "value1", "key2": "value2"}


def test_parse_variable_placeholder_with_additional_text() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.variables = {"VAR1": "VALUE_OF_VAR1", "VAR2": "VALUE_OF_VAR2"}

    result = yaml_parser.substitute_variable_placeholder(
        node=Mock(value="This is a test: ${{ variables.VAR1 }} + ${{ variables.VAR2 }}")
    )

    assert result == "This is a test: VALUE_OF_VAR1 + VALUE_OF_VAR2"


def test_parse_variable_placeholder_not_defined() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.variables = {"VAR1": "VALUE_OF_VAR1"}

    with pytest.raises(ValueError, match="Variable `VAR2` is not set."):
        yaml_parser.substitute_variable_placeholder(node=Mock(value="${{ variables.VAR2 }}"))


def test_parse_secrets_placeholder_success() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.secrets = {
        "secret_identifier": SimpleSecretPlugin(
            plugin_id="test123", secret_name="my-secret-aws-secret", region="aws-region"
        )
    }

    result = yaml_parser.substitute_secret_placeholder(node=Mock(value="${{ secrets.secret_identifier }}"))

    assert result == "super_secret_value"


def test_parse_secrets_placeholder_not_defined() -> None:
    yaml_parser = ExtendedCoreLoader(stream=Mock())
    yaml_parser.secrets = {}

    with pytest.raises(ValueError, match="Secret `db_password` is not set."):
        yaml_parser.substitute_secret_placeholder(node=Mock(value="${{ secrets.db_password }}"))


def test_parse_yaml_text_success(yaml_pipeline: str) -> None:
    serialized_yaml = YamlParser(yaml_pipeline).yaml_body

    expected_dict = {
        "pipelines": {
            "pipeline1": {
                "type": "ETL",
                "phases": {
                    "extract": {
                        "steps": [{"id": "mock_extract1", "plugin": "mock_s3"}],
                    },
                    "transform": {
                        "steps": [
                            {
                                "id": "mock_tranformation1",
                                "plugin": "aggregate_sum",
                            }
                        ],
                    },
                    "load": {
                        "steps": [{"id": "mock_load1", "plugin": "mock_s3"}],
                    },
                },
            }
        }
    }

    assert serialized_yaml == expected_dict, (
        f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"
    )


def test_parse_yaml_file_success(yaml_pipeline: str, tmpdir) -> None:
    file = tmpdir.mkdir("sub").join("data.yaml")
    file.write_text(yaml_pipeline, encoding="utf-8")

    file_obj = Path(file.strpath).open("r")  # noqa: SIM115 - Context Handler exists inside YamlParser

    serialized_yaml = YamlParser(file_obj).yaml_body

    expected_dict = {
        "pipelines": {
            "pipeline1": {
                "type": "ETL",
                "phases": {
                    "extract": {
                        "steps": [{"id": "mock_extract1", "plugin": "mock_s3"}],
                    },
                    "transform": {
                        "steps": [
                            {
                                "id": "mock_tranformation1",
                                "plugin": "aggregate_sum",
                            }
                        ],
                    },
                    "load": {
                        "steps": [{"id": "mock_load1", "plugin": "mock_s3"}],
                    },
                },
            }
        }
    }

    assert serialized_yaml == expected_dict, (
        f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"
    )
