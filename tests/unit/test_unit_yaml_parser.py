# Standard Imports
import os
import tempfile
from typing import Generator
from unittest.mock import Mock

# Third Party Imports
import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.parsers.yaml_parser import ExtendedCoreLoader, YamlParser


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


@pytest.fixture(scope="session")
def temporary_yaml_file(yaml_pipeline: str) -> Generator[str]:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".yaml") as temp_file:
        yaml_content = yaml.safe_load(yaml_pipeline)

        with open(temp_file.name, "w") as f:  # noqa: PTH123
            yaml.dump(yaml_content, f)

        yield temp_file.name

        os.remove(temp_file.name)  # noqa: PTH107


def test_parse_invalid_yaml() -> None:
    yaml_content = """
    key1: value1
    key2 value2  # Missing colon
    key3:
    - item1
    - item2:
    """

    with pytest.raises(yaml.YAMLError):
        YamlParser.from_text(yaml_content)


@pytest.mark.asyncio
async def test_parse_yaml_file_not_found() -> None:
    with pytest.raises(FileNotFoundError, match="File not found: not_found.yaml"):
        await YamlParser.from_file(file_path="not_found.yaml")


def test_parse_env_var_name_success() -> None:
    mock_stream = Mock()
    match_mock = Mock().return_value
    match_mock.group.return_value = "${{ env.ENV_VAR1 }}"

    assert ExtendedCoreLoader.parse_env_var_name(mock_stream(), match_mock) == "ENV_VAR1"
    mock_stream.assert_called_once()


def test_parse_env_variables_in_yaml(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("ENV_VAR1", "VALUE_OF_ENV_1")
    monkeypatch.setenv("ENV_VAR2", "VALUE_OF_ENV_2")

    yaml_with_env_vars = """
    value1: ${{ env.ENV_VAR1 }}
    value2: ${{ env.ENV_VAR2 }}
    """

    parsed_yaml = YamlParser.from_text(yaml_with_env_vars).content

    assert parsed_yaml["value1"] == "VALUE_OF_ENV_1"
    assert parsed_yaml["value2"] == "VALUE_OF_ENV_2"


def test_parse_env_variables_multiple_occurrences(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("ENV_VAR1", "VALUE_OF_ENV_1")

    yaml_with_env_vars = """
    value1: ${{ env.ENV_VAR1 }}
    value2: ${{ env.ENV_VAR1 }}
    """

    parsed_yaml = YamlParser.from_text(yaml_with_env_vars).content

    assert parsed_yaml["value1"] == "VALUE_OF_ENV_1"
    assert parsed_yaml["value2"] == "VALUE_OF_ENV_1"


def test_parse_env_variables_without_env_prefix(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("ENV_VAR1", "VALUE_OF_ENV_1")

    yaml_with_env_vars = """
    value1: ${{ ENV_VAR1 }}
    """

    YamlParser.from_text(yaml_with_env_vars)


def test_env_var_not_defined(mocker: MockerFixture) -> None:
    parse_env_mock = mocker.patch.object(ExtendedCoreLoader, "parse_env_var_name", return_value="ENV_VAR1")

    yaml_with_env_vars = """
    value1: ${{ env.ENV_VAR1 }}
    """
    with pytest.raises(ValueError, match="Environment variable `ENV_VAR1` is not set."):
        YamlParser.from_text(yaml_with_env_vars)

    parse_env_mock.assert_called_once()


def test_parse_yaml_text_success(yaml_pipeline: str) -> None:
    serialized_yaml = YamlParser.from_text(yaml_pipeline).content

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


@pytest.mark.asyncio
async def test_parse_yaml_file_success(temporary_yaml_file: str) -> None:
    serialized_yaml = await YamlParser.from_file(temporary_yaml_file)

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

    assert serialized_yaml.content == expected_dict, (
        f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"
    )
