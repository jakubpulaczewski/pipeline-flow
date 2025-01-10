# Standard Imports
import os
# Third-party Imports
import pytest


# Project Imports
from plugins.registry import PluginRegistry, PluginWrapper

from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.models.pipeline import Pipeline

from tests.resources.mocks import (
    mock_extractor,
    mock_loader,
    mock_transformer,
    mock_load_transformer,
    mock_merger
)
@pytest.fixture(autouse=True)
def setup_logging_level():
    os.environ["LOG_LEVEL"] = "debug"

@pytest.fixture(autouse=True)
def plugin_registry_setup():
    PluginRegistry._registry = {}  # Ensure a clean state before each test
    yield
    PluginRegistry._registry = {}  # Clean up after each test


@pytest.fixture
def extractor_plugin_data():
    return {
        "id": "extractor_id",
        "plugin": "mock_extractor",
    }


@pytest.fixture
def loader_plugin_data():
    return {"id": "loader_id", "plugin": "mock_loader"}

@pytest.fixture
def second_loader_plugin_data():
    return {"id": "loader_id_2", "plugin": "mock_loader_2"}


@pytest.fixture
def loader_mock(loader_plugin_data):
    return mock_loader(id=loader_plugin_data['id'])


@pytest.fixture
def second_loader_mock(second_loader_plugin_data):
    return mock_loader(id=second_loader_plugin_data["id"])


@pytest.fixture
def transformer_plugin_data():
    return {"id": "transformer_id", "plugin": "mock_transformer"}


@pytest.fixture
def second_transformer_plugin_data():
    return {"id": "transformer_id_2", "plugin": "mock_transformer_2"}


@pytest.fixture
def transformer_mock(transformer_plugin_data):
    return mock_transformer(id=transformer_plugin_data['id'])

@pytest.fixture
def second_transformer_mockr(second_transformer_plugin_data):
    return mock_transformer(id=second_transformer_plugin_data['id'])


@pytest.fixture
def transform_at_load_plugin_data():
    return {"id": "mock_transform_loader_id", "plugin": 'mock_transformer_loader'}

@pytest.fixture
def second_transform_at_load_plugin_data():
    return {"id": "mock_transform_loader_id_2", "plugin": 'mock_transformer_loader_2'}


@pytest.fixture
def load_transformer_mock(transform_at_load_plugin_data):
    return mock_load_transformer(id=transform_at_load_plugin_data['id'])

@pytest.fixture
def second_load_transformer_mock(second_transform_at_load_plugin_data):
    return mock_load_transformer(id=second_transform_at_load_plugin_data['id'])

## TODO: Re check all o f this after.
@pytest.fixture
def merger_plugin_data():
    return {
        'plugin': 'mock_merger'
    }

@pytest.fixture
def merger_mock(merger_plugin_data):
    return mock_merger()


def pipeline_factory(default_config):
    # Factory function for creating pipelines
    def create_pipeline(**overrides):
        config = default_config.copy()
        config.update(overrides)

        phases = {
            "extract": ExtractPhase.model_construct(steps=config.get("extract")),
            "transform": TransformPhase.model_construct(steps=config.get("transform")) if config['type'] == "ETL" or config['type'] == "ETLT" else None,
            "load": LoadPhase.model_construct(steps=config.get("load")),
            "transform_at_load": TransformLoadPhase.model_construct(steps=config.get("transform_at_load")) if config['type'] == "ELT" or config['type'] == "ETLT" else None,
        }

        # Include only-non empty values
        phases = {k: v for k, v in phases.items() if v}
        print(phases)

        return Pipeline(
            name=config["name"],
            description=config.get("description", ""),
            type=config["type"],
            needs=config["needs"],
            phases=phases,
        )

    return create_pipeline



@pytest.fixture
def etl_pipeline_factory(request):
    default_config = {
        "name": "ETL Pipeline",
        "type": "ETL",
        "extract": [],
        "transform": [],
        "load": [],
        "needs": None
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline


@pytest.fixture
def elt_pipeline_factory(request):
    default_config = {
        "name": "ELT Pipeline",
        "type": "ELT",
        "extract": [],
        "load": [],
        "transform_at_load": [],
        "needs": None
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline

@pytest.fixture
def etlt_pipeline_factory(request):
    default_config = {
        "name": "ETLT Pipeline",
        "type": "ETLT",
        "extract": [],
        "transform": [],
        "load": [],
        "transform_at_load": [],
        "needs": None
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline


# @pytest.fixture
# def flexible_pipeline_factory():
#     def _flexible_pipeline_factory(
#         name: str,
#         pipeline_type: str = "ETL",
#         extract: list = None,
#         transform: list = None,
#         load: list = None,
#         transform_at_load: list = None,
#         needs: list[str] = None,
#     ):
#         default_config = {
#             "name": name,
#             "type": pipeline_type,
#             "extract": extract or [],
#             "transform": transform or [],
#             "load": load or [],
#             "transform_at_load": transform_at_load or [],
#             "needs": needs,
#         }

#         return pipeline_factory(default_config)

#     return _flexible_pipeline_factory
