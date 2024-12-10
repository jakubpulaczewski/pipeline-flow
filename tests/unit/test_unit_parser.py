# Standard Imports
import tempfile
import os
import yaml
from unittest.mock import call, patch

# Third-party Imports
import pytest

from pydantic import ValidationError

# Project Imports
import core.parser as parser  # pylint: disable=consider-using-from-import
from core.models.pipeline import Pipeline
from core.models.phases import PipelinePhase
from plugins.registry import PluginFactory
from tests.common.constants import ETL, EXTRACT_PHASE, TRANSFORM_PHASE, LOAD_PHASE
from tests.common.mocks import MockExtractor, MockLoad, MockTransform


@pytest.fixture(scope='session')
def yaml_pipeline():
    yaml_str = """
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

    return yaml_str

@pytest.fixture(scope='session')
def temporary_yaml_file(yaml_pipeline):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml")

    yaml_content = yaml.safe_load(yaml_pipeline)

    with open(temp_file.name, "w") as f:
        yaml.dump(yaml_content, f)
    
    yield temp_file.name

    os.remove(temp_file.name)


def test_parse_empty_yaml():
    with pytest.raises(ValueError):
        parser.parse_yaml_str("")

def test_parse_invalid_yaml():
    yaml_content = """
    key1: value1
    key2 value2  # Missing colon
    key3:
    - item1
    - item2:
    """
    
    with pytest.raises(yaml.YAMLError):
        parser.parse_yaml_str(yaml_content)


def test_parse_yaml_file_not_found():
    with pytest.raises(FileNotFoundError, match="File not found: not_found.yaml"):
        parser.parse_yaml_file("not_found.yaml")



def test_parse_yaml_str_success(yaml_pipeline) -> None:
    serialized_yaml = parser.parse_yaml_str(yaml_pipeline)

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
                    }
                }
            }
        }
    }

    assert (
        serialized_yaml == expected_dict
    ), f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"


def test_parse_yaml_file_success(temporary_yaml_file) -> None:
    serialized_yaml = parser.parse_yaml_file(temporary_yaml_file)

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
                    }
                }
            }
        }
    }
    

    assert (
        serialized_yaml == expected_dict
    ), f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"


def test_parse_one_plugin(extractor_plugin_data):
    phase_data = {
        "steps": [extractor_plugin_data]
    }

    with patch.object(PluginFactory, "get", return_value=MockExtractor) as mock:
        result = parser.parse_plugins_by_phase(EXTRACT_PHASE, phase_data["steps"])

        assert len(result) == 1
        assert result[0] == MockExtractor(
            id="extractor_id", plugin="mock_extractor", config=None
        )

        mock.assert_called_with(EXTRACT_PHASE, "mock_extractor")


def test_parse_multiple_plugins(extractor_plugin_data, second_extractor_plugin_data):
    phase_data = {
        "steps": [
            extractor_plugin_data,
            second_extractor_plugin_data
        ]
    }

    with patch.object(
        PluginFactory, "get", side_effect=[MockExtractor, MockExtractor]
    ) as mock:
        result = parser.parse_plugins_by_phase(EXTRACT_PHASE, phase_data["steps"])

        assert len(result) == 2
        assert result[0] == MockExtractor(
            id="extractor_id", config=None
        )
        assert result[1] == MockExtractor(
            id="extractor_id_2", config=None
        )

        assert call(EXTRACT_PHASE, "mock_extractor") in mock.mock_calls
        assert call(EXTRACT_PHASE, "mock_extractor_2") in mock.mock_calls


def test_create_pipeline_with_no_pipeline_attributes():
    pipeline_data = {}

    with pytest.raises(ValueError):
        parser.create_pipeline("empty_pipeline", pipeline_data)


def test_create_pipelines_from_empty_dict():
    with pytest.raises(ValueError, match="Pipeline attributes are empty"):
        parser.parse_all_pipelines({})



def test_create_pipeline_with_only_mandatory_phases(
    mocker,
    extractor_plugin_data,
    second_extractor_plugin_data,
    loader_plugin_data,
    second_loader_plugin_data
):
    pipeline_data = {
        "type": "ETL",
        "phases": {
            "extract": {
                "steps": [
                    extractor_plugin_data, second_extractor_plugin_data
                ]
            },
            "transform": {
                "steps": []
            },
            "load": {
                "steps": [
                    loader_plugin_data, second_loader_plugin_data
                ]
            }
        }
    }

    mocker.patch.object(
        PluginFactory,
        "get",
        side_effect=[
            MockExtractor,
            MockExtractor,
            MockLoad,
            MockLoad,
        ],
    )

    pipeline = parser.create_pipeline("full_pipeline", pipeline_data)
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"

    assert len(pipeline.extract.steps) == 2
    assert isinstance(pipeline.extract.steps[0], MockExtractor)
    assert isinstance(pipeline.extract.steps[1], MockExtractor)

    assert len(pipeline.transform.steps) == 0

    assert len(pipeline.load.steps) == 2
    assert isinstance(pipeline.load.steps[0], MockLoad)
    assert isinstance(pipeline.load.steps[1], MockLoad)



def test_create_pipeline_without_mandatory_phase(
    mocker,
    loader_plugin_data,
    second_loader_plugin_data
):
    pipeline_data = {
        "type": "ETL",
        "phases": {
            "extract": {
                "steps": []
            },
            "transform": {
                "steps": []
            },
            "load": {
                "steps": [
                    loader_plugin_data, second_loader_plugin_data
                ]
            }
        }
    }

    mocker.patch.object(
        PluginFactory,
        "get",
        side_effect=[
            MockLoad,
            MockLoad,
        ],
    )

    with pytest.raises(ValidationError, match="Validation Failed! Mandatory phase 'PipelinePhase.EXTRACT_PHASE' cannot be empty or missing plugins."):
        parser.create_pipeline("full_pipeline", pipeline_data)

def test_create_pipeline_with_multiple_sources_destinations(
    mocker,
    extractor_plugin_data,
    second_extractor_plugin_data,
    transformer_plugin_data,
    transformer_plugin_data_2,
    loader_plugin_data,
    second_loader_plugin_data
    ):
    pipeline_data = {
        "type": "ETL",
        "phases": {
            "extract": {
                "steps": [
                    extractor_plugin_data, second_extractor_plugin_data
                ]
            },
            "transform": {
                "steps": [
                    transformer_plugin_data, transformer_plugin_data_2
                ]
            },
            "load": {
                "steps": [
                    loader_plugin_data, second_loader_plugin_data
                ]
            }
        }
    }

    mocker.patch.object(
        PluginFactory,
        "get",
        side_effect=[
            MockExtractor,
            MockExtractor,
            MockTransform,
            MockTransform,
            MockLoad,
            MockLoad,
        ],
    )

    pipeline = parser.create_pipeline("full_pipeline", pipeline_data)
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"

    assert len(pipeline.extract.steps) == 2
    assert isinstance(pipeline.extract.steps[0], MockExtractor)
    assert isinstance(pipeline.extract.steps[1], MockExtractor)

    assert len(pipeline.transform.steps) == 2
    assert isinstance(pipeline.transform.steps[0], MockTransform)
    assert isinstance(pipeline.transform.steps[1], MockTransform)

    assert len(pipeline.load.steps) == 2
    assert isinstance(pipeline.load.steps[0], MockLoad)
    assert isinstance(pipeline.load.steps[1], MockLoad)

