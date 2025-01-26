# Standard Imports

# Third Party Imports
import pytest
from pytest_mock import MockerFixture

# Project Imports
from core.models.phases import ExtractPhase, LoadPhase, TransformPhase
from core.models.pipeline import Pipeline, PipelineType
from core.parsers import pipeline_parser
from core.plugins import PluginWrapper
from tests.resources import mocks


def test_create_pipeline_with_no_pipeline_attributes() -> None:
    with pytest.raises(ValueError, match="Pipeline attributes are empty"):
        pipeline_parser._create_pipeline(pipeline_name="pipeline_2", pipeline_data={})  # type: ignore[reportFunctionMemberAccess]


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
                    {"id": "extractor_id_2", "plugin": "mock_extractor_2"},
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
        "_create_phase",
        side_effect=[
            ExtractPhase.model_construct(
                steps=[
                    PluginWrapper(id="extractor_id", func=mocks.mock_extractor("extractor_id")),
                    PluginWrapper(
                        id="extractor_id_2",
                        func=mocks.mock_extractor("extractor_id_2"),
                    ),
                ]
            ),
            TransformPhase.model_construct(steps=[]),
            LoadPhase.model_construct(
                steps=[
                    PluginWrapper(id="loader_id", func=mocks.mock_loader("loader_id")),
                    PluginWrapper(id="loader_id_2", func=mocks.mock_loader("loader_id_2")),
                ]
            ),
        ],
    )

    pipeline = pipeline_parser._create_pipeline("full_pipeline", pipeline_data)  # type: ignore[reportFunctionMemberAccess]

    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"
    assert pipeline.type == PipelineType.ETL
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 2
    assert pipeline.extract.steps[0].id == "extractor_id"
    assert pipeline.extract.steps[1].id == "extractor_id_2"

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
                    {"id": "extractor_id_2", "plugin": "mock_extractor_2"},
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
        "_create_phase",
        side_effect=[
            ExtractPhase.model_construct(
                steps=[
                    PluginWrapper(id="extractor_id", func=mocks.mock_extractor("extractor_id")),
                    PluginWrapper(
                        id="extractor_id_2",
                        func=mocks.mock_extractor("extractor_id_2"),
                    ),
                ]
            ),
            TransformPhase.model_construct(
                steps=[
                    PluginWrapper(
                        id="transformer_id",
                        func=mocks.mock_transformer("transformer_id"),
                    ),
                    PluginWrapper(
                        id="transformer_id_2",
                        func=mocks.mock_transformer("transformer_id_2"),
                    ),
                ]
            ),
            LoadPhase.model_construct(
                steps=[
                    PluginWrapper(id="loader_id", func=mocks.mock_loader("loader_id")),
                    PluginWrapper(id="loader_id_2", func=mocks.mock_loader("loader_id_2")),
                ]
            ),
        ],
    )

    pipeline = pipeline_parser._create_pipeline("full_pipeline", pipeline_data)  # type: ignore[reportFunctionMemberAccess]
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"

    assert len(pipeline.extract.steps) == 2
    assert pipeline.extract.steps[0].id == "extractor_id"
    assert pipeline.extract.steps[1].id == "extractor_id_2"

    assert len(pipeline.transform.steps) == 2
    assert pipeline.transform.steps[0].id == "transformer_id"
    assert pipeline.transform.steps[1].id == "transformer_id_2"

    assert len(pipeline.load.steps) == 2
    assert pipeline.load.steps[0].id == "loader_id"
    assert pipeline.load.steps[1].id == "loader_id_2"
