# Standard Imports

# Third Party Imports
import pytest

# Project Imports
from pipeline_flow.common.type_def import SyncPlugin
from pipeline_flow.core.models.phases import PipelinePhase
from pipeline_flow.core.models.pipeline import Pipeline
from pipeline_flow.core.parsers import parse_pipelines
from pipeline_flow.core.plugins import PluginRegistry, PluginWrapper
from tests.resources.plugins import (
    simple_extractor_plugin,
    simple_loader_plugin,
    simple_transform_load_plugin,
    simple_transform_plugin,
)


def setup_plugins(plugin_dict: dict[PipelinePhase, list[tuple[str, SyncPlugin]]]) -> None:
    for phase, plugins in plugin_dict.items():
        for plugin_name, plugin_callable in plugins:
            PluginRegistry.register(phase, plugin_name, plugin_callable)


def test_parse_pipeline_without_pipelines() -> None:
    pipelines_data = {}
    with pytest.raises(ValueError, match="No Pipelines detected."):
        parse_pipelines(pipelines_data)


def test_parse_pipeline_without_registered_plugins() -> None:
    pipeline_data = {
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

    with pytest.raises(
        ValueError,
        match="Plugin class was not found for following plugin `mock_s3`.",
    ):
        parse_pipelines(pipeline_data)  # type: ignore[reportFunctionMemberAccess]


def test_parse_etl_pipeline_with_missing_extract_phase() -> None:
    # Register Plugins
    plugins = {
        PipelinePhase.TRANSFORM_PHASE: [("transform_plugin", simple_transform_plugin)],
        PipelinePhase.LOAD_PHASE: [("load_plugin", simple_loader_plugin)],
    }
    setup_plugins(plugins)

    pipeline_data = {
        "pipeline1": {
            "type": "ETL",
            "phases": {
                "transform": {
                    "steps": [
                        {
                            "id": "mock_tranformation1",
                            "plugin": "transform_plugin",
                        }
                    ],
                },
                "load": {
                    "steps": [{"id": "mock_load1", "plugin": "load_plugin"}],
                },
            },
        }
    }

    with pytest.raises(
        ValueError,
        match=(
            "Validation Error: The provided phases do not match the required phases for pipeline type "
            "'ETL'. Missing phases: {<PipelinePhase.EXTRACT_PHASE: 'extract'>}."
        ),
    ):
        parse_pipelines(pipeline_data)  # type: ignore[reportFunctionMemberAccess]


def test_parse_etl_pipeline_with_extra_phases() -> None:
    # Register Plugins
    plugins = {
        PipelinePhase.EXTRACT_PHASE: [("extractor_plugin", simple_extractor_plugin)],
        PipelinePhase.TRANSFORM_PHASE: [("transform_plugin", simple_transform_plugin)],
        PipelinePhase.LOAD_PHASE: [("load_plugin", simple_loader_plugin)],
        PipelinePhase.TRANSFORM_AT_LOAD_PHASE: [("transform_at_load_plugin", simple_transform_load_plugin)],
    }
    setup_plugins(plugins)

    pipeline_data = {
        "pipeline1": {
            "type": "ETL",
            "phases": {
                "extract": {
                    "steps": [{"id": "mock_extract1", "plugin": "extractor_plugin"}],
                },
                "transform": {
                    "steps": [
                        {
                            "id": "mock_tranformation1",
                            "plugin": "transform_plugin",
                        }
                    ],
                },
                "load": {
                    "steps": [{"id": "mock_load1", "plugin": "load_plugin"}],
                },
                "transform_at_load": {
                    "steps": [
                        {
                            "id": "mock_transfor_at_load",
                            "plugin": "transform_at_load_plugin",
                            "params": {"query": "SELECT 13"},
                        },
                    ],
                },
            },
        }
    }

    with pytest.raises(
        ValueError,
        match="Extra phases: {<PipelinePhase.TRANSFORM_AT_LOAD_PHASE: 'transform_at_load'>}",
    ):
        parse_pipelines(pipeline_data)  # type: ignore[reportFunctionMemberAccess]


def test_parse_etl_pipeline_with_only_mandatory_phases() -> None:
    # Register Plugins
    plugins = {
        PipelinePhase.EXTRACT_PHASE: [("extractor_plugin", simple_extractor_plugin)],
        PipelinePhase.LOAD_PHASE: [("load_plugin", simple_loader_plugin)],
    }
    setup_plugins(plugins)

    pipeline_data = {
        "pipeline1": {
            "type": "ETL",
            "phases": {
                "extract": {
                    "steps": [{"id": "mock_extract1", "plugin": "extractor_plugin"}],
                },
                "load": {
                    "steps": [{"id": "mock_load1", "plugin": "load_plugin"}],
                },
            },
        }
    }
    pipelines = parse_pipelines(pipeline_data)  # type: ignore[reportFunctionMemberAccess]

    pipeline = pipelines[0]

    assert len(pipelines) == 1
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "pipeline1"

    assert len(pipeline.extract.steps) == 1
    assert pipeline.extract.steps[0] == PluginWrapper(id="mock_extract1", func=simple_extractor_plugin())

    assert len(pipeline.load.steps) == 1
    assert pipeline.load.steps[0] == PluginWrapper(id="mock_load1", func=simple_loader_plugin())


def test_parse_etl_multiple_pipelines() -> None:
    # Register Required Plugins
    plugins = {
        PipelinePhase.EXTRACT_PHASE: [
            ("extract_plugin1", simple_extractor_plugin),
        ],
        PipelinePhase.TRANSFORM_PHASE: [("aggregate_sum_etl", simple_transform_plugin)],
        PipelinePhase.LOAD_PHASE: [
            ("load_plugin1", simple_loader_plugin),
            ("load_plugin2", simple_loader_plugin),
        ],
    }
    setup_plugins(plugins)

    pipelines_data = {
        "pipeline1": {
            "type": "ETL",
            "phases": {
                "extract": {"steps": [{"id": "mock_extract1", "plugin": "extract_plugin1"}]},
                "transform": {"steps": [{"id": "mock_tranformation1", "plugin": "aggregate_sum_etl"}]},
                "load": {"steps": [{"id": "mock_load1", "plugin": "load_plugin1"}]},
            },
        },
        "pipeline2": {
            "type": "ETL",
            "phases": {
                "extract": {"steps": [{"id": "mock_extract2", "plugin": "extract_plugin1"}]},
                "load": {"steps": [{"id": "mock_load2", "plugin": "load_plugin2"}]},
            },
        },
    }

    pipelines = parse_pipelines(pipelines_data)  # type: ignore[reportFunctionMemberAccess]

    assert len(pipelines) == 2
    assert isinstance(pipelines[0], Pipeline)
    assert isinstance(pipelines[1], Pipeline)

    # Pipeline 1
    assert len(pipelines[0].extract.steps) == 1
    assert pipelines[0].extract.steps[0] == PluginWrapper(id="mock_extract1", func=simple_extractor_plugin())

    assert len(pipelines[0].transform.steps) == 1
    assert pipelines[0].transform.steps[0] == PluginWrapper(
        id="mock_tranformation1",
        func=simple_transform_plugin(),
    )

    assert len(pipelines[0].load.steps) == 1
    assert pipelines[0].load.steps[0] == PluginWrapper(id="mock_load1", func=simple_loader_plugin())

    # Pipeline 2
    assert len(pipelines[1].extract.steps) == 1
    assert pipelines[1].extract.steps[0] == PluginWrapper(id="mock_extract2", func=simple_extractor_plugin())

    assert len(pipelines[1].load.steps) == 1
    assert pipelines[1].load.steps[0] == PluginWrapper(id="mock_load2", func=simple_loader_plugin())


def test_parse_elt_pipeline() -> None:
    # Register Required Plugins
    plugins = {
        PipelinePhase.EXTRACT_PHASE: [
            ("extract_plugin1", simple_extractor_plugin),
        ],
        PipelinePhase.LOAD_PHASE: [
            ("load_plugin1", simple_loader_plugin),
        ],
        PipelinePhase.TRANSFORM_AT_LOAD_PHASE: [("upsert_transformation", simple_transform_load_plugin)],
    }
    setup_plugins(plugins)

    pipeline_data = {
        "pipeline1": {
            "type": "ELT",
            "phases": {
                "extract": {"steps": [{"id": "mock_extract1", "plugin": "extract_plugin1"}]},
                "load": {"steps": [{"id": "mock_load1", "plugin": "load_plugin1"}]},
                "transform_at_load": {
                    "steps": [
                        {
                            "id": "mock_load_transformer1",
                            "plugin": "upsert_transformation",
                            "params": {"query": "Select 2"},
                        }
                    ]
                },
            },
        }
    }

    pipelines = parse_pipelines(pipeline_data)  # type: ignore[reportFunctionMemberAccess]

    assert len(pipelines) == 1
    assert isinstance(pipelines[0], Pipeline)
    assert pipelines[0].name == "pipeline1"

    assert len(pipelines[0].extract.steps) == 1
    assert pipelines[0].extract.steps[0] == PluginWrapper(id="mock_extract1", func=simple_extractor_plugin())

    assert len(pipelines[0].load.steps) == 1
    assert pipelines[0].load.steps[0] == PluginWrapper(id="mock_load1", func=simple_loader_plugin())

    assert len(pipelines[0].load_transform.steps) == 1
    assert pipelines[0].load_transform.steps[0] == PluginWrapper(
        id="mock_load_transformer1",
        func=simple_transform_load_plugin(query="Select 2"),
    )


def test_parse_etlt_pipeline() -> None:
    # Setup Required Plugins
    plugins = {
        PipelinePhase.EXTRACT_PHASE: [
            ("extract_plugin1", simple_extractor_plugin),
        ],
        PipelinePhase.TRANSFORM_PHASE: [("transform_plugin", simple_transform_plugin)],
        PipelinePhase.LOAD_PHASE: [
            ("load_plugin1", simple_loader_plugin),
        ],
        PipelinePhase.TRANSFORM_AT_LOAD_PHASE: [("upsert_transformation", simple_transform_load_plugin)],
    }

    setup_plugins(plugins)

    pipelines_data = {
        "pipeline_ETLT": {
            "type": "ETLT",
            "phases": {
                "extract": {"steps": [{"id": "mock_extract1", "plugin": "extract_plugin1"}]},
                "transform": {"steps": [{"id": "mock_tranformation1", "plugin": "transform_plugin"}]},
                "load": {"steps": [{"id": "mock_load1", "plugin": "load_plugin1"}]},
                "transform_at_load": {
                    "steps": [
                        {
                            "id": "mock_load_transformer1",
                            "plugin": "upsert_transformation",
                            "params": {"query": "Select 1"},
                        }
                    ]
                },
            },
        }
    }

    pipelines = parse_pipelines(pipelines_data)  # type: ignore[reportFunctionMemberAccess]

    assert len(pipelines) == 1
    assert isinstance(pipelines[0], Pipeline)
    assert pipelines[0].name == "pipeline_ETLT"

    assert len(pipelines[0].extract.steps) == 1
    assert pipelines[0].extract.steps[0] == PluginWrapper(id="mock_extract1", func=simple_extractor_plugin())

    assert len(pipelines[0].transform.steps) == 1
    assert pipelines[0].transform.steps[0] == PluginWrapper(
        id="mock_tranformation1",
        func=simple_transform_plugin(),
    )

    assert len(pipelines[0].load.steps) == 1
    assert pipelines[0].load.steps[0] == PluginWrapper(id="mock_load1", func=simple_loader_plugin())

    assert len(pipelines[0].load_transform.steps) == 1
    assert pipelines[0].load_transform.steps[0] == PluginWrapper(
        id="mock_load_transformer1",
        func=simple_transform_load_plugin(query="Select 1"),
    )
