# Standard Imports

# Third Party Imports
import pytest
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.models.phases import ExtractPhase, LoadPhase, Phase, TransformLoadPhase, TransformPhase
from pipeline_flow.core.models.pipeline import Pipeline, PipelineType
from pipeline_flow.core.parsers import pipeline_parser
from pipeline_flow.core.registry import PluginRegistry
from tests.resources.plugins import (
    IPlugin,
    SimpleExtractorPlugin,
    SimpleLoaderPlugin,
    SimplePostPlugin,
    SimpleTransformLoadPlugin,
    SimpleTransformPlugin,
)


@pytest.mark.parametrize(
    ("pipeline_phase", "expected", "plugin_callable"),
    [
        ("extract", ExtractPhase, SimpleExtractorPlugin),
        ("transform", TransformPhase, SimpleTransformPlugin),
        ("load", LoadPhase, SimpleLoaderPlugin),
    ],
)
def test_parse_phase(
    pipeline_phase: str, expected: type[Phase], plugin_callable: IPlugin, mocker: MockerFixture
) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=plugin_callable)
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


def test_parse_load_phase_with_post_processing(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", side_effect=[SimpleLoaderPlugin, SimplePostPlugin])
    phase_details = {
        "steps": [
            {
                "id": "mock_id",
                "plugin": "mock_plugin",
            }
        ],
        "post": [{"id": "post_plugin", "plugin": "simple_post_plugin"}],
    }
    parsed_phase = pipeline_parser._parse_phase("load", phase_details)

    assert len(parsed_phase.steps) == 1
    assert isinstance(parsed_phase, LoadPhase)
    assert isinstance(parsed_phase.steps[0], SimpleLoaderPlugin)
    assert isinstance(parsed_phase.post[0], SimplePostPlugin)


def test_parse_transform_load_phase(mocker: MockerFixture) -> None:
    # Since TransformLoadPhase requires an extra parameter (e.g. "query"), it is better handled in its own test.
    mocker.patch.object(PluginRegistry, "get", return_value=SimpleTransformLoadPlugin)
    phase_details = {
        "steps": [
            {
                "id": "mock_id",
                "plugin": "mock_plugin",
                "args": {
                    "query": "SELECT 1",
                },
            }
        ],
    }
    parsed_phase = pipeline_parser._parse_phase("transform_at_load", phase_details)

    assert len(parsed_phase.steps) == 1
    assert isinstance(parsed_phase, TransformLoadPhase)


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
            ExtractPhase.model_construct(steps=[SimpleExtractorPlugin(plugin_id="extractor_id")]),
            TransformPhase.model_construct(steps=[]),
            LoadPhase.model_construct(
                steps=[
                    SimpleLoaderPlugin(plugin_id="loader_id"),
                    SimpleLoaderPlugin(plugin_id="loader_id_2"),
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
    assert isinstance(pipeline.extract.steps[0], SimpleExtractorPlugin)
    assert pipeline.extract.steps[0].id == "extractor_id"

    assert len(pipeline.transform.steps) == 0

    assert len(pipeline.load.steps) == 2
    assert isinstance(pipeline.load.steps[0], SimpleLoaderPlugin)
    assert isinstance(pipeline.load.steps[1], SimpleLoaderPlugin)
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
                    SimpleExtractorPlugin(plugin_id="extractor_id"),
                ]
            ),
            TransformPhase.model_construct(
                steps=[
                    SimpleTransformPlugin(plugin_id="transformer_id"),
                    SimpleTransformPlugin(plugin_id="transformer_id_2"),
                ]
            ),
            LoadPhase.model_construct(
                steps=[
                    SimpleLoaderPlugin(plugin_id="loader_id"),
                    SimpleLoaderPlugin(plugin_id="loader_id_2"),
                ]
            ),
        ],
    )

    pipeline = pipeline_parser._create_pipeline("full_pipeline", pipeline_data)  # type: ignore[reportFunctionMemberAccess]
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "full_pipeline"

    assert len(pipeline.extract.steps) == 1
    assert pipeline.extract.steps[0].id == "extractor_id"
    assert isinstance(pipeline.extract.steps[0], SimpleExtractorPlugin)

    assert len(pipeline.transform.steps) == 2
    assert isinstance(pipeline.transform.steps[0], SimpleTransformPlugin)
    assert isinstance(pipeline.transform.steps[1], SimpleTransformPlugin)
    assert pipeline.transform.steps[0].id == "transformer_id"
    assert pipeline.transform.steps[1].id == "transformer_id_2"

    assert len(pipeline.load.steps) == 2
    assert isinstance(pipeline.load.steps[0], SimpleLoaderPlugin)
    assert isinstance(pipeline.load.steps[1], SimpleLoaderPlugin)
    assert pipeline.load.steps[0].id == "loader_id"
    assert pipeline.load.steps[1].id == "loader_id_2"
