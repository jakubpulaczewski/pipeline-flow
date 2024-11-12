# Standard Imports

# Third-party imports
import pytest

# Project Imports
from core.parser import create_pipelines_from_dict, deserialize_yaml
from core.plugins import PluginFactory

from core.models.extract import ExtractPhase
from core.models.load import LoadPhase
from core.models.transform import TransformPhase

from tests.common.constants import EXTRACT_PHASE, LOAD_PHASE
from tests.common.mocks import MockExtractor, MockLoad


@pytest.fixture(autouse=True)
def plugin_setup():
    PluginFactory._registry = {}
    yield
    PluginFactory._registry = {}


def setup_plugins(plugin_dict):
    for phase, plugins in plugin_dict.items():
        for (plugin_name, plugin_callable) in plugins:
            PluginFactory.register(phase, plugin_name, plugin_callable)


def test_parse_etl_pipeline_with_empty_mandatory_phases() -> None:
    yaml_str = """
    pipelines:
        pipeline1:
            type: ETL
            extract:
                steps: []
            transform:
                steps: []
            load:
                steps: []
    """

    deserialiazed_yaml = deserialize_yaml(yaml_str)

    with pytest.raises(ValueError, match="Validation Failed! Mandatory phase '%s' cannot be empty or missing plugins."):
        create_pipelines_from_dict(deserialiazed_yaml.get('pipelines'))


def test_parse_etl_without_initialised_plugins() -> None:
    yaml_str = """
    pipelines:
        pipeline1:
            type: ETL
            extract:
                steps:
                  - id: mock_extract1
                    type: extract_plugin
            transform:
                steps: []
            load:
                steps:
                  - id: mock_load1
                    type: load_plugin

    """

    deserialiazed_yaml = deserialize_yaml(yaml_str)

    with pytest.raises(ValueError, match="Plugin class '%s' was not found"):
        create_pipelines_from_dict(deserialiazed_yaml.get('pipelines'))


def test_parse_etl_single_pipeline(plugin_setup) -> None:
    # Setup Required Plugins
    plugins = {
        EXTRACT_PHASE: [('extract_plugin', MockExtractor)],
        LOAD_PHASE: [('load_plugin', MockLoad)]
    }
    setup_plugins(plugins)

    yaml_str = """
    pipelines:
        pipeline1:
            type: ETL
            extract:
                steps:
                  - id: mock_extract1
                    type: extract_plugin
            transform:
                steps: []
            load:
                steps:
                  - id: mock_load1
                    type: load_plugin

    """

    deserialiazed_yaml = deserialize_yaml(yaml_str)

    pipelines = create_pipelines_from_dict(deserialiazed_yaml.get('pipelines'))
    pipeline = pipelines[0]

    assert len(pipelines) == 1

    assert pipeline.name == 'pipeline1'
    assert pipeline.needs == None
    assert pipeline.type == 'ETL'

    assert pipeline.extract == ExtractPhase(steps=[MockExtractor(id='mock_extract1', type='extract_plugin')])
    assert pipeline.transform == TransformPhase(steps=[])
    assert pipeline.load == LoadPhase(steps=[MockLoad(id='mock_load1', type='load_plugin')])


def test_parse_etl_multiple_extract_and_load(plugin_setup) -> None:
    # Setup Required Plugins
    plugins = {
        EXTRACT_PHASE: [
            ('extract_plugin1', MockExtractor),
            ('extract_plugin2', MockExtractor),
        ],
        LOAD_PHASE: [
            ('load_plugin1', MockLoad),
            ('load_plugin2', MockLoad),
        ]
    }
    setup_plugins(plugins)
    
    yaml_str = """
    pipelines:
        pipeline1:
            type: ETL
            extract:
                steps:
                  - id: mock_extract1
                    type: extract_plugin1

                  - id: mock_extract2
                    type: extract_plugin2
            transform:
                steps: []
            load:
                steps:
                  - id: mock_load1
                    type: load_plugin1
                  - id: mock_load2
                    type: load_plugin2

    """
    deserialiazed_yaml = deserialize_yaml(yaml_str)

    pipelines = create_pipelines_from_dict(deserialiazed_yaml.get('pipelines'))
    pipeline = pipelines[0]

    assert len(pipelines) == 1

    assert pipeline.name == 'pipeline1'
    assert pipeline.needs == None
    assert pipeline.type == 'ETL'

    assert pipeline.extract == ExtractPhase(
        steps=[
            MockExtractor(id='mock_extract1', type='extract_plugin1'),
            MockExtractor(id='mock_extract2', type='extract_plugin2')
    ])
    assert pipeline.transform == TransformPhase(steps=[])
    assert pipeline.load == LoadPhase(
        steps=[
            MockLoad(id='mock_load1', type='load_plugin1'),
            MockLoad(id='mock_load2', type='load_plugin2')
        ])
