# Standard Imports
import os
# Third-party Imports
import pytest


# Project Imports
from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.models.pipeline import Pipeline
from plugins.registry import  PluginFactory

from tests.resources.mocks import (
    MockExtractor, 
    MockLoad, 
    MockTransform, 
    MockLoadTransform
)
@pytest.fixture(autouse=True)
def setup_logging_level():
    os.environ["LOG_LEVEL"] = "debug"

@pytest.fixture(autouse=True)
def plugin_registry_setup():
    PluginFactory._registry = {}  # Ensure a clean state before each test
    yield
    PluginFactory._registry = {}  # Clean up after each test


@pytest.fixture
def extractor_plugin_data():
    return {
        "id": "extractor_id",
        "plugin": "mock_extractor",
    }

@pytest.fixture
def extractor_mock(extractor_plugin_data):
    return MockExtractor(id=extractor_plugin_data['id'])


@pytest.fixture
def second_extractor_plugin_data():
    return {
        "id": "extractor_id_2", 
        "plugin": "mock_extractor_2"
    }

@pytest.fixture
def second_extractor_mock(second_extractor_plugin_data):
    return MockExtractor(id=second_extractor_plugin_data['id'])


@pytest.fixture
def loader_plugin_data():
    return {"id": "loader_id", "plugin": "mock_loader"}

@pytest.fixture
def second_loader_plugin_data():
    return {"id": "loader_id_2", "plugin": "mock_loader_2"}


@pytest.fixture
def mock_loader(loader_plugin_data):
    return MockLoad(id=loader_plugin_data['id'])

@pytest.fixture
def second_mock_loader(second_loader_plugin_data):
    return MockLoad(id=second_loader_plugin_data["id"])


@pytest.fixture
def transformer_plugin_data():
    return {"id": "transformer_id", "plugin": "mock_transformer"}


@pytest.fixture
def second_transformer_plugin_data():
    return {"id": "transformer_id2", "plugin": "mock_transformer_2"}


@pytest.fixture
def mock_transformer(transformer_plugin_data):
    return MockTransform(id=transformer_plugin_data['id'])

@pytest.fixture
def second_mock_transformer(second_transformer_plugin_data):
    return MockTransform(id=second_transformer_plugin_data['id'])


@pytest.fixture
def transform_at_load_plugin_data():
    return {"id": "mock_transform_load_id"}

@pytest.fixture
def second_transform_at_load_plugin_data():
    return {"id": "mock_transform_load_id_2"}


@pytest.fixture
def mock_load_transformer(transform_at_load_plugin_data):
    return MockLoadTransform(id=transform_at_load_plugin_data['id'])

@pytest.fixture
def second_mock_load_transformer(second_transform_at_load_plugin_data):
    return MockLoadTransform(id=second_transform_at_load_plugin_data['id'])


def pipeline_factory(default_config):
    # Factory function for creating pipelines
    def create_pipeline(**overrides):
        config = default_config.copy()
        config.update(overrides)

        phases = {
            "extract": ExtractPhase(steps=config.get("extract")),
            "transform": TransformPhase(steps=config.get("transform")) if config['type'] == "ETL" or config['type'] == "ETLT" else None,
            "load": LoadPhase(steps=config.get("load")),
            "transform_at_load": TransformLoadPhase(steps=config.get("transform_at_load")) if config['type'] == "ELT" or config['type'] == "ETLT" else None,
        }

        # Include only-non empty values
        phases = {k: v for k, v in phases.items() if v}

        return Pipeline(
            name=config["name"],
            description=config.get("description", ""),
            type=config["type"],
            needs=config["needs"],
            phases=phases,
        )

    return create_pipeline


@pytest.fixture
def etl_pipeline_factory(request, extractor_mock, mock_transformer, mock_loader):
    default_config = {
        "name": "ETL Pipeline",
        "type": "ETL",
        "extract": [extractor_mock],
        "transform": [mock_transformer],
        "load": [mock_loader],
        "needs": None
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline


@pytest.fixture
def elt_pipeline_factory(
    request, extractor_mock,  mock_loader, mock_load_transformer
):
    default_config = {
        "name": "ELT Pipeline",
        "type": "ELT",
        "extract": [extractor_mock],
        "load": [mock_loader],
        "transform_at_load": [mock_load_transformer],
        "needs": None
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline

@pytest.fixture
def etlt_pipeline_factory(
    request, extractor_mock,  mock_transformer, mock_loader, mock_load_transformer
):
    default_config = {
        "name": "ETLT Pipeline",
        "type": "ETLT",
        "extract": [extractor_mock],
        "transform": [mock_transformer],
        "load": [mock_loader],
        "transform_at_load": [mock_load_transformer],
        "needs": None   
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline