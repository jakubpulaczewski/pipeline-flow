# Standard Imports

# Third Party Imports
import pytest

# Project Imports
from core.models.phases import ExtractPhase, LoadPhase, Phase, TransformLoadPhase, TransformPhase
from core.models.pipeline import Pipeline, PipelineType
from core.parsers import pipeline_parser
from core.plugins import PluginRegistry, PluginWrapper
from pytest_mock import MockerFixture

from tests.resources.plugins import simple_dummy_plugin


@pytest.mark.parametrize(
    ("pipeline_phase", "expected"),
    [
        ("extract", ExtractPhase),
        ("transform", TransformPhase),
        ("load", LoadPhase),
        ("transform_at_load", TransformLoadPhase),
    ],
)
def test_parse_phase(pipeline_phase: str, expected: type[Phase], mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=simple_dummy_plugin)
    phase_details = {
        "steps": [
            {
                "id": "mock_id",
                "plugin": "mock_plugin",
            }
        ]
    }
    parsed_phase = pipeline_parser._parse_phase(pipeline_phase, phase_details)

    assert len(parsed_phase.steps) == 1
    assert isinstance(parsed_phase, expected)


def test_create_pipeline_with_no_pipeline_attributes() -> None:
    with pytest.raises(ValueError, match="Pipeline attributes are empty"):
        pipeline_parser._create_pipeline("pipeline_2", {})  # type: ignore[reportFunctionMemberAccess]


def test_create_pipeline_with_only_mandatory_phases(mocker: MockerFixture) -> None:
    pipeline_data = {
        "type": "ETL",
        "phases": {
            "extract": {
                "steps": [
                    {
                        "id": "extractor_id",
                        "plugin": "mock_extractor",
                    },
                ]
            },
            "transform": {"steps": []},
            "load": {
                "steps": [
                    {"id": "loader_id", "plugin": "mock_loader"},
                    {"id": "loader_id_2", "plugin": "mock_loader_2"},
                ]
            },
        },
    }

    mocker.patch.object(
        pipeline_parser,
        "_parse_phase",
        side_effect=[
            ExtractPhase.model_construct(
                steps=[
                    PluginWrapper(id="extractor_id", func=simple_dummy_plugin()),
                ]
            ),
            TransformPhase.model_construct(steps=[]),
            LoadPhase.model_construct(
                steps=[
                    PluginWrapper(id="loader_id", func=simple_dummy_plugin()),
                    PluginWrapper(id="loader_id_2", func=simple_dummy_plugin()),
                ]
            ),
        ],
    )

    pipeline = pipeline_parser._create_pipeline("full_pipeline", pipeline_data)  # type: ignore[reportFunctionMemberAccess]

    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"
    assert pipeline.type == PipelineType.ETL
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert pipeline.extract.steps[0].id == "extractor_id"

    assert len(pipeline.transform.steps) == 0

    assert len(pipeline.load.steps) == 2
    assert pipeline.load.steps[0].id == "loader_id"
    assert pipeline.load.steps[1].id == "loader_id_2"


def test_create_pipeline_with_multiple_sources_destinations(mocker: MockerFixture) -> None:
    pipeline_data = {
        "type": "ETL",
        "phases": {
            "extract": {
                "steps": [
                    {
                        "id": "extractor_id",
                        "plugin": "mock_extractor",
                    },
                ]
            },
            "transform": {
                "steps": [
                    {"id": "transformer_id", "plugin": "mock_transformer"},
                    {"id": "transformer_id_2", "plugin": "mock_transformer_2"},
                ]
            },
            "load": {
                "steps": [
                    {"id": "loader_id", "plugin": "mock_loader"},
                    {"id": "loader_id_2", "plugin": "mock_loader_2"},
                ]
            },
        },
    }

    mocker.patch.object(
        pipeline_parser,
        "_parse_phase",
        side_effect=[
            ExtractPhase.model_construct(
                steps=[
                    PluginWrapper(id="extractor_id", func=simple_dummy_plugin()),
                ]
            ),
            TransformPhase.model_construct(
                steps=[
                    PluginWrapper(
                        id="transformer_id",
                        func=simple_dummy_plugin(),
                    ),
                    PluginWrapper(
                        id="transformer_id_2",
                        func=simple_dummy_plugin(),
                    ),
                ]
            ),
            LoadPhase.model_construct(
                steps=[
                    PluginWrapper(id="loader_id", func=simple_dummy_plugin),
                    PluginWrapper(id="loader_id_2", func=simple_dummy_plugin),
                ]
            ),
        ],
    )

    pipeline = pipeline_parser._create_pipeline("full_pipeline", pipeline_data)  # type: ignore[reportFunctionMemberAccess]
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"

    assert len(pipeline.extract.steps) == 1
    assert pipeline.extract.steps[0].id == "extractor_id"

    assert len(pipeline.transform.steps) == 2
    assert pipeline.transform.steps[0].id == "transformer_id"
    assert pipeline.transform.steps[1].id == "transformer_id_2"

    assert len(pipeline.load.steps) == 2
    assert pipeline.load.steps[0].id == "loader_id"
    assert pipeline.load.steps[1].id == "loader_id_2"
