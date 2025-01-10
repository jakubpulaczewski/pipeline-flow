# Standard Imports
import os
# Third-party Imports
import pytest


# Project Imports
import tests.resources.mocks as mocks

from core.plugins import PluginRegistry, PluginWrapper

from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.models.pipeline import Pipeline


@pytest.fixture(autouse=True)
def setup_logging_level():
    os.environ["LOG_LEVEL"] = "debug"

@pytest.fixture(autouse=True)
def plugin_registry_setup():
    PluginRegistry._registry = {}  # Ensure a clean state before each test
    yield
    PluginRegistry._registry = {}  # Clean up after each test


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
        "extract": [PluginWrapper(id='mock_extractor', func=mocks.mock_extractor(id='mock_extractor'))],
        "transform": [PluginWrapper(id='mock_transformer', func=mocks.mock_transformer(id='mock_transformer'))],
        "load": [PluginWrapper(id='mock_loader', func=mocks.mock_loader(id='mock_loader'))],
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
        "extract": [PluginWrapper(id='mock_extractor', func=mocks.mock_extractor(id='mock_extractor'))],
        "load": [PluginWrapper(id='mock_loader', func=mocks.mock_loader(id='mock_loader'))],
        "transform_at_load": [PluginWrapper(id='mock_load_transformer', func=mocks.mock_load_transformer(id='mock_load_transformer', query="SELECT 1"))],
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
        "extract": [PluginWrapper(id='mock_extractor', func=mocks.mock_extractor(id='mock_extractor'))],
        "transform": [PluginWrapper(id='mock_transformer', func=mocks.mock_transformer(id='mock_transformer'))],
        "load": [PluginWrapper(id='mock_loader', func=mocks.mock_loader(id='mock_loader'))],
        "transform_at_load": [PluginWrapper(id='mock_load_transformer', func=mocks.mock_load_transformer(id='mock_load_transformer', query="SELECT 1"))],
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
