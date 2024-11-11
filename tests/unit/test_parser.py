# Standard Imports
from unittest.mock import Mock, patch

# Third-party Imports
import pytest

# Project Imports
import core.parser as parser  # pylint: disable=consider-using-from-import
from core.models.extract import IExtractor
from core.models.load import ILoader
from core.models.pipeline import Pipeline
from core.models.transform import ITransformerETL
from core.plugins import PluginFactory
from tests.common.constants import EXTRACT_PHASE, LOAD_PHASE


def test_deserialize_empty_yaml():
    """Given that empty YAML string is passed to the deserialize function, it should raise an exception."""
    with pytest.raises(ValueError):
        parser.deserialize_yaml("")


def test_deserialize_valid_yaml() -> None:
    """A test that verifies the serialization of the YAML."""
    yaml_str = """
    pipelines:
        pipeline1:
            name: Pipeline_1
            type: ELT
            extract:
                para1: val1
                steps:
                  - id: mock_extract1
                    type: mock_s3
            transform:
                para2: val2
                steps:
                  - id: mock_tranformation1
            load:
                para3: val3
                steps:
                  - id: mock_load1
                    type: mock_s3
    """

    serialized_yaml = parser.deserialize_yaml(yaml_str)

    expected_dict = {
        "pipelines": {
            "pipeline1": {
                "name": "Pipeline_1",
                "type": "ELT",
                "extract": {
                    "para1": "val1",
                    "steps": [
                        {
                            "id": "mock_extract1",
                            "type": "mock_s3",
                        }
                    ],
                },
                "transform": {
                    "para2": "val2",
                    "steps": [
                        {
                            "id": "mock_tranformation1",
                        }
                    ],
                },
                "load": {
                    "para3": "val3",
                    "steps": [
                        {
                            "id": "mock_load1",
                            "type": "mock_s3",
                        }
                    ],
                },
            }
        }
    }

    assert (
        serialized_yaml == expected_dict
    ), f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"


@pytest.mark.parametrize("mandatory_phase", [(EXTRACT_PHASE), (LOAD_PHASE)])
def test_validate_phase_configuration_with_missing_phases(mandatory_phase: str):
    "When there are missing phases present, the validation should raise ValueError."
    with pytest.raises(ValueError):
        parser.validate_phase_configuration(mandatory_phase, {})


@pytest.mark.parametrize("phase", [(EXTRACT_PHASE), (LOAD_PHASE)])
def test_validate_phase_configuration_with_mandatory_phases_present(phase):
    "When mandatory phases are present, the validation should be passed."
    phase_args = {
        "steps": [
            {
                "id": f"mock_{phase}",
                "type": "mock_s3",
            }
        ],
        "another_phase_para": "value1",
    }
    assert parser.validate_phase_configuration(phase, phase_args) == True


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


def test_parse_one_plugin():
    phase_data = {
        "steps": [
            {
                "id": "mock_extractor",
                "type": "mock_s3",
            }
        ]
    }

    MockPluginA = Mock(spec=IExtractor)

    with patch.object(PluginFactory, "get", return_value=MockPluginA):
        result = parser.parse_phase_steps_plugins(EXTRACT_PHASE, phase_data)

        assert len(result) == 1
        assert result[0] is MockPluginA


def test_parse_multiple_plugins():
    phase_data = {
        "steps": [
            {
                "id": "mock_extractor",
                "type": "mock",
            },
            {"id": "mock_extractor", "type": "mock"},
        ]
    }

    MockExtractorA = Mock(spec=IExtractor)
    MockExtractorB = Mock(spec=IExtractor)

    with patch.object(
        PluginFactory, "get", side_effect=[MockExtractorA, MockExtractorB]
    ):
        result = parser.parse_phase_steps_plugins(EXTRACT_PHASE, phase_data)

        assert len(result) == 2
        assert result[0] is MockExtractorA
        assert result[1] is MockExtractorB


def test_create_pipeline_with_no_pipeline_attributes():
    pipeline_data = {}

    with pytest.raises(ValueError):
        parser.create_pipeline_from_data("empty_pipeline", pipeline_data)


def test_create_pipelines_from_empty_dict():
    assert parser.create_pipelines_from_dict({}) == []


def test_create_pipeline_with_multiple_sources_destinations(mocker):
    pipeline_data = {
        "type": "ETL",
        "extract": {
            "steps": [
                {
                    "id": "mock_extractor_1",
                    "type": "mock_s3",
                },
                {"id": "mock_extractor_2", "type": "mock_jdbc"},
            ]
        },
        "transform": {
            "steps": [
                {
                    "id": "transformation1",
                },
                {
                    "id": "transformation2",
                },
            ]
        },
        "load": {
            "steps": [
                {
                    "id": "mock_load_1",
                    "type": "mock_s3",
                },
                {"id": "mock_load2", "type": "mock_jdbc"},
            ]
        },
    }

    MockExtractor = Mock(spec=IExtractor)
    MockTranformer = Mock(spec=ITransformerETL)
    MockLoader = Mock(spec=ILoader)

    mocker.patch.object(
        PluginFactory,
        "get",
        side_effect=[
            MockExtractor,
            MockExtractor,
            MockTranformer,
            MockTranformer,
            MockLoader,
            MockLoader,
        ],
    )

    pipeline = parser.create_pipeline_from_data("full_pipeline", pipeline_data)
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"
    assert (
        len(pipeline.extract.steps) == 2 and pipeline.extract.steps[0] is MockExtractor
    )
    assert (
        len(pipeline.transform.steps) == 2
        and pipeline.transform.steps[0] is MockTranformer
    )
    assert len(pipeline.load.steps) == 2 and pipeline.load.steps[0] is MockLoader
