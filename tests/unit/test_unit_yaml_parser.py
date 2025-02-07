# Standard Imports
import os
import tempfile
from typing import Generator

# Third Party Imports
import pytest
import yaml

# Project Imports
from pipeline_flow.core.parsers import YamlParser


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


class TestUnitYamlParser:
    @staticmethod
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

    @staticmethod
    @pytest.mark.asyncio
    async def test_parse_yaml_file_not_found() -> None:
        with pytest.raises(FileNotFoundError, match="File not found: not_found.yaml"):
            await YamlParser.from_file(file_path="not_found.yaml")

    @staticmethod
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

    @staticmethod
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
