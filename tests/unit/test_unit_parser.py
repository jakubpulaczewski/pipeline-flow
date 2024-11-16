# Standard Imports
from unittest.mock import call, patch

# Third-party Imports
import pytest

from pydantic import ValidationError

# Project Imports
import core.parser as parser  # pylint: disable=consider-using-from-import
from core.models.pipeline import Pipeline
from core.models.phases import PipelinePhase
from core.plugins import PluginFactory
from tests.common.constants import ETL, EXTRACT_PHASE, TRANSFORM_PHASE, LOAD_PHASE
from tests.common.mocks import MockExtractor, MockLoad, MockTransform


def test_deserialize_empty_yaml():
    """Given that empty YAML string is passed to the deserialize function, it should raise an exception."""
    with pytest.raises(ValueError):
        parser.deserialize_yaml("")


def test_deserialize_valid_yaml() -> None:
    yaml_str = f"""
    pipelines:
        pipeline1:
            type: ETL
            phases:
                - extract:
                    steps:
                        - id: mock_extract1
                          plugin: mock_s3
                - transform:
                    steps:
                        - id: mock_tranformation1
                          operation: aggregate_sum
                - load:
                    steps:
                        - id: mock_load1
                          type: mock_s3
    """

    serialized_yaml = parser.deserialize_yaml(yaml_str)

    expected_dict = {
        "pipelines": {
            "pipeline1": {
                "type": "ETL",
                "phases": [
                    {
                        "extract": {
                            "steps": [{"id": "mock_extract1", "plugin": "mock_s3"}],
                        }
                    },
                    {
                        "transform": {
                            "steps": [
                                {
                                    "id": "mock_tranformation1",
                                    "operation": "aggregate_sum",
                                }
                            ],
                        }
                    },
                    {
                        "load": {
                            "steps": [{"id": "mock_load1", "type": "mock_s3"}],
                        }
                    },
                ],
            }
        }
    }

    assert (
        serialized_yaml == expected_dict
    ), f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"


def test_parse_plugins_with_missing_args_mandatory_phase(mocker):
    mocker.patch.object(
        parser,
        "parse_phase_steps_plugins",
        side_effect=ValueError(
            "Validation Failed! Mandatory phase EXTRACT_PHASE cannot be empty or missing plugins."
        ),
    )
    phase_data = {EXTRACT_PHASE: {}}

    with pytest.raises(ValueError):
        parser.parse_phase_steps_plugins(EXTRACT_PHASE, phase_data)


def test_parse_one_plugin(extractor_plugin_data):
    phase_data = {
        "steps": [extractor_plugin_data]
    }

    with patch.object(PluginFactory, "get", return_value=MockExtractor) as mock:
        result = parser.parse_phase_steps_plugins(EXTRACT_PHASE, phase_data)

        assert len(result) == 1
        assert result[0] == MockExtractor(
            id="extractor_id", plugin="mock_extractor", config=None
        )

        mock.assert_called_with(EXTRACT_PHASE, "mock_extractor")


def test_parse_multiple_plugins(extractor_plugin_data, extractor_plugin_data_2):
    phase_data = {
        "steps": [
            extractor_plugin_data,
            extractor_plugin_data_2
        ]
    }

    with patch.object(
        PluginFactory, "get", side_effect=[MockExtractor, MockExtractor]
    ) as mock:
        result = parser.parse_phase_steps_plugins(EXTRACT_PHASE, phase_data)

        assert len(result) == 2
        assert result[0] == MockExtractor(
            id="extractor_id", plugin="mock_extractor", config=None
        )
        assert result[1] == MockExtractor(
            id="extractor_id_2", plugin="mock_extractor_2", config=None
        )

        assert call(EXTRACT_PHASE, "mock_extractor") in mock.mock_calls
        assert call(EXTRACT_PHASE, "mock_extractor_2") in mock.mock_calls


def test_create_pipeline_with_no_pipeline_attributes():
    pipeline_data = {}

    with pytest.raises(ValueError):
        parser.create_pipeline_from_data("empty_pipeline", pipeline_data)


def test_create_pipelines_from_empty_dict():
    assert parser.create_pipelines_from_dict({}) == []



def test_create_pipeline_with_only_mandatory_phases(
    mocker,
    extractor_plugin_data,
    extractor_plugin_data_2,
    loader_plugin_data,
    loader_plugin_data_2
):
    pipeline_data = {
        "type": "ETL",
        "phases": [
        {
            "extract": {
                "steps": [
                    extractor_plugin_data, extractor_plugin_data_2
                ]
            },
            "transform": {
                "steps": []
            },
            "load": {
                "steps": [
                    loader_plugin_data, loader_plugin_data_2
                ]
            }
        }]
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

    pipeline = parser.create_pipeline_from_data("full_pipeline", pipeline_data)
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"

    assert len(pipeline.phases[0][EXTRACT_PHASE].steps) == 2
    assert isinstance(pipeline.phases[0][EXTRACT_PHASE].steps[0], MockExtractor)
    assert isinstance(pipeline.phases[0][EXTRACT_PHASE].steps[1], MockExtractor)

    assert len(pipeline.phases[0][TRANSFORM_PHASE].steps) == 0

    assert len(pipeline.phases[0][LOAD_PHASE].steps) == 2
    assert isinstance(pipeline.phases[0][LOAD_PHASE].steps[0], MockLoad)
    assert isinstance(pipeline.phases[0][LOAD_PHASE].steps[1], MockLoad)



def test_create_pipeline_without_mandatory_phase(
    mocker,
    loader_plugin_data,
    loader_plugin_data_2
):
    pipeline_data = {
        "type": "ETL",
        "phases": [
        {
            "extract": {
                "steps": []
            },
            "transform": {
                "steps": []
            },
            "load": {
                "steps": [
                    loader_plugin_data, loader_plugin_data_2
                ]
            }
        }]
    }

    mocker.patch.object(
        PluginFactory,
        "get",
        side_effect=[
            MockLoad,
            MockLoad,
        ],
    )

    with pytest.raises(ValidationError, match="Validation Failed! Mandatory phase '%s' cannot be empty or missing plugins."):
        parser.create_pipeline_from_data("full_pipeline", pipeline_data)

def test_create_pipeline_with_multiple_sources_destinations(
    mocker,
    extractor_plugin_data,
    extractor_plugin_data_2,
    transformer_plugin_data,
    transformer_plugin_data_2,
    loader_plugin_data,
    loader_plugin_data_2
    ):
    pipeline_data = {
        "type": "ETL",
        "phases": [
        {
            "extract": {
                "steps": [
                    extractor_plugin_data, extractor_plugin_data_2
                ]
            },
            "transform": {
                "steps": [
                    transformer_plugin_data, transformer_plugin_data_2
                ]
            },
            "load": {
                "steps": [
                    loader_plugin_data, loader_plugin_data_2
                ]
            }
        }]
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

    pipeline = parser.create_pipeline_from_data("full_pipeline", pipeline_data)
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"

    assert len(pipeline.phases[0][EXTRACT_PHASE].steps) == 2
    assert isinstance(pipeline.phases[0][EXTRACT_PHASE].steps[0], MockExtractor)
    assert isinstance(pipeline.phases[0][EXTRACT_PHASE].steps[1], MockExtractor)

    assert len(pipeline.phases[0][TRANSFORM_PHASE].steps) == 2
    assert isinstance(pipeline.phases[0][TRANSFORM_PHASE].steps[0], MockTransform)
    assert isinstance(pipeline.phases[0][TRANSFORM_PHASE].steps[1], MockTransform)

    assert len(pipeline.phases[0][LOAD_PHASE].steps) == 2
    assert isinstance(pipeline.phases[0][LOAD_PHASE].steps[0], MockLoad)
    assert isinstance(pipeline.phases[0][LOAD_PHASE].steps[1], MockLoad)

