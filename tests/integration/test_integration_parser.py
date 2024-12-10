# Standard Imports

# Third-party imports
import pytest

from core.models.phases import ExtractPhase, LoadPhase, TransformPhase, TransformLoadPhase

# Project Imports
import core.parser as parser
from core.models.pipeline import Pipeline
from plugins.registry import PluginFactory

from tests.common.constants import (
    EXTRACT_PHASE, 
    TRANSFORM_PHASE,
    LOAD_PHASE,
    LOAD_TRANSFORM_PHASE
)
from tests.common.mocks import MockExtractor, MockTransform, MockLoad, MockLoadTransform


@pytest.fixture(autouse=True)
def plugin_setup():
    PluginFactory._registry = {}
    yield
    PluginFactory._registry = {}


def setup_plugins(plugin_dict):
    for phase, plugins in plugin_dict.items():
        for plugin_name, plugin_callable in plugins:
            PluginFactory.register(phase, plugin_name, plugin_callable)



def test_parse_etl_without_initialised_plugins() -> None:
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


    with pytest.raises(ValueError, match="Plugin class '%s' was not found"):
        parser.start_parser(yaml_str)

def test_parse_etl_pipeline_with_empty_mandatory_phase() -> None:
    plugins = {
        TRANSFORM_PHASE: [("transform_plugin", MockTransform)],
        LOAD_PHASE: [("load_plugin", MockLoad)],
    }
    setup_plugins(plugins)
    
    yaml_str = """
    pipelines:
      pipeline1:
        type: ETL
        phases:
          transform:
            steps:
              - id: mock_tranformation1
                plugin: transform_plugin
          load:
            steps:
              - id: mock_load1
                plugin: load_plugin
    """


    with pytest.raises(
        ValueError,
        match="Validation Failed! Mandatory phase 'PipelinePhase.EXTRACT_PHASE' cannot be empty or missing plugins.",
    ):
        parser.start_parser(yaml_str)

def test_parse_etl_pipeline_with_only_mandatory_phases() -> None:

    plugins = {
        EXTRACT_PHASE: [("extractor_plugin", MockExtractor)],
        LOAD_PHASE: [("load_plugin", MockLoad)],
    }
    setup_plugins(plugins)
    
    yaml_str = """
    pipelines:
      pipeline1:
        type: ETL
        phases:
          extract:
            steps:
              - id: mock_extract1
                plugin: extractor_plugin
          load:
            steps:
              - id: mock_load1
                plugin: load_plugin
    """

    pipelines = parser.start_parser(yaml_str)
    pipeline = pipelines[0]

    assert len(pipelines) == 1
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "pipeline1"


    assert len(pipeline.phases[EXTRACT_PHASE].steps) == 1
    assert isinstance(pipeline.phases[EXTRACT_PHASE].steps[0], MockExtractor)

    assert len(pipeline.phases[LOAD_PHASE].steps) == 1
    assert isinstance(pipeline.phases[LOAD_PHASE].steps[0], MockLoad)



def test_parse_etl_multiple_pipelines() -> None:
    # Setup Required Plugins
    plugins = {
        EXTRACT_PHASE: [
            ("extract_plugin1", MockExtractor),
        ],
        TRANSFORM_PHASE: [
            ("aggregate_sum_etl", MockTransform)
        ],
        LOAD_PHASE: [
            ("load_plugin1", MockLoad),
            ("load_plugin2", MockLoad),
        ],
    }
    setup_plugins(plugins)

    yaml_str = """
    pipelines:
      pipeline1:
        type: ETL
        phases:
          extract:
            steps:
              - id: mock_extract1
                plugin: extract_plugin1
          transform:
            steps:
              - id: mock_tranformation1
                plugin: aggregate_sum_etl
          load:
            steps:
              - id: mock_load1
                plugin: load_plugin1
      pipeline2:
        type: ETL
        phases:
          extract:
            steps:
              - id: mock_extract2
                plugin: extract_plugin1
          load:
            steps:
              - id: mock_load2
                plugin: load_plugin2
    """

    pipelines = parser.start_parser(yaml_str)

    assert len(pipelines) == 2
    assert isinstance(pipelines[0], Pipeline)
    assert isinstance(pipelines[1], Pipeline)

    # Pipeline 1
    assert len(pipelines[0].phases[EXTRACT_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[EXTRACT_PHASE].steps[0], MockExtractor)

    assert len(pipelines[0].phases[TRANSFORM_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[TRANSFORM_PHASE].steps[0], MockTransform)

    assert len(pipelines[0].phases[LOAD_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[LOAD_PHASE].steps[0], MockLoad)

   # Pipeline 2
    assert len(pipelines[1].phases[EXTRACT_PHASE].steps) == 1
    assert isinstance(pipelines[1].phases[EXTRACT_PHASE].steps[0], MockExtractor)

    assert len(pipelines[1].phases[LOAD_PHASE].steps) == 1
    assert isinstance(pipelines[1].phases[LOAD_PHASE].steps[0], MockLoad)




def test_parse_elt_pipeline() -> None:
    # Setup Required Plugins
    plugins = {
        EXTRACT_PHASE: [
            ("extract_plugin1", MockExtractor),
        ],
        LOAD_PHASE: [
            ("load_plugin1", MockLoad),
        ],        
        LOAD_TRANSFORM_PHASE: [
            ("upsert_transformation", MockLoadTransform)
        ]

    }
    setup_plugins(plugins)

    yaml_str = """
    pipelines:
      pipeline1:
        type: ETL
        phases:
          extract:
            steps: 
              - id: mock_extract1
                plugin: extract_plugin1
          load:
            steps:
              - id: mock_load1
                plugin: load_plugin1
          transform_at_load:
            steps:
              - id: mock_tranformation1
                plugin: upsert_transformation
    """


    pipelines = parser.start_parser(yaml_str)

    assert len(pipelines) == 1
    assert isinstance(pipelines[0], Pipeline)
    assert pipelines[0].name == "pipeline1"


    assert len(pipelines[0].phases[EXTRACT_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[EXTRACT_PHASE].steps[0], MockExtractor)

    assert len(pipelines[0].phases[LOAD_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[LOAD_PHASE].steps[0], MockLoad)


    assert len(pipelines[0].phases[LOAD_TRANSFORM_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[LOAD_TRANSFORM_PHASE].steps[0], MockLoadTransform)




def test_parse_etlt_pipeline() -> None:
    # Setup Required Plugins
    plugins = {
        EXTRACT_PHASE: [
            ("extract_plugin1", MockExtractor),
        ],
        TRANSFORM_PHASE: [
            ("transform_plugin", MockTransform)
        ],
        LOAD_PHASE: [
            ("load_plugin1", MockLoad),
        ],
        LOAD_TRANSFORM_PHASE: [
            ("upsert_transformation", MockLoadTransform)
        ]
    }

    setup_plugins(plugins)

    yaml_str = """
    pipelines:
      pipeline_ETLT:
        type: ETLT
        phases:
          extract:
            steps: 
              - id: mock_extract1
                plugin: extract_plugin1
          transform:
            steps:
              - id: mock_tranformation1
                plugin: transform_plugin
          load:
            steps:
              - id: mock_load1
                plugin: load_plugin1
          transform_at_load:
            steps:
              - id: mock_tranformation1
                plugin: upsert_transformation
    """

    pipelines = parser.start_parser(yaml_str)

    assert len(pipelines) == 1
    assert isinstance(pipelines[0], Pipeline)
    assert pipelines[0].name == "pipeline_ETLT"


    assert len(pipelines[0].phases[EXTRACT_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[EXTRACT_PHASE].steps[0], MockExtractor)

    assert len(pipelines[0].phases[TRANSFORM_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[TRANSFORM_PHASE].steps[0], MockTransform)

    assert len(pipelines[0].phases[LOAD_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[LOAD_PHASE].steps[0], MockLoad)


    assert len(pipelines[0].phases[LOAD_TRANSFORM_PHASE].steps) == 1
    assert isinstance(pipelines[0].phases[LOAD_TRANSFORM_PHASE].steps[0], MockLoadTransform)

