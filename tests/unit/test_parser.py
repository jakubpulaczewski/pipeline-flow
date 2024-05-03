# Standard Imports
from unittest.mock import patch, Mock

# Third-party Imports
import pytest
import pydantic as pyd

# Project Imports
import core.parser as parser  # pylint: disable=consider-using-from-import
from core.plugins import PluginFactory
from core.models import Pipeline



def test_deserialize_valid_yaml() -> None:
    """A test that verifies the serialization of the YAML."""
    yaml_str = """
    pipelines:
        pipeline1:
            name: Name
            extract: []
            transform: []
            load: []
    """

    serialized_yaml = parser.deserialize_yaml(yaml_str)

    expected_dict = {
        "pipelines": {"pipeline1": {"name": "Name", "extract": [], "transform": [], "load": []}}
    }
    assert serialized_yaml == expected_dict


def test_deserialize_empty_yaml():
    assert parser.deserialize_yaml('') == None

def test_parse_plugins_for_empty_stage():
    stage_data = {'extract': []}
    assert parser.parse_plugins_for_etl_stage(stage_data, 'extract') == []


def test_parse_plugins_for_multiple_plugins():
    stage_data = {'load': [{'type': 'pluginA'}, {'type': 'pluginB'}]}
    
    MockPluginA = Mock(name='MockPluginA')
    MockPluginB = Mock(name='MockPluginB')

    with patch.object(PluginFactory, 'get', side_effect=[MockPluginA, MockPluginB]):
        result = parser.parse_plugins_for_etl_stage(stage_data, 'load')
        assert len(result) == 2
        assert result[0] == MockPluginA and result[1] == MockPluginB


def test_create_pipeline_with_no_stages():
    pipeline_data = {}

    with pytest.raises(pyd.ValidationError):
        parser.create_pipeline_from_data('empty_pipeline', pipeline_data)


def test_create_pipeline_with_multiple_stages():
    pipeline_data = {'type': 'ELT', 'extract': [], 'transform': [], 'load': []}
    pipeline = parser.create_pipeline_from_data('full_pipeline', pipeline_data)
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == 'full_pipeline' and all([hasattr(pipeline, stage) for stage in ['extract', 'transform', 'load']])


def test_create_pipelines_from_empty_dict():
    assert parser.create_pipelines_from_dict({}) == []

def test_create_multiple_pipelines_from_dict():
    pipelines_dict = {
        'pipeline1': {'type': 'ELT', 'extract': [], 'transform': [], 'load': []},
        'pipeline2': {'type': 'ELT', 'extract': [], 'transform': [], 'load': []},
    }
    pipelines = parser.create_pipelines_from_dict(pipelines_dict)
    assert len(pipelines) == 2
    assert all(isinstance(pipeline, Pipeline) for pipeline in pipelines)

