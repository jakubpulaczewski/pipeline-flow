# Standard Imports
import os
from collections.abc import Callable
from typing import Any, Generator

# Third-party Imports
import pytest

from common.logger import setup_logger
from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.models.pipeline import Pipeline
from core.plugins import PluginRegistry, PluginWrapper

# Project Imports
from tests.resources.plugins import (
    simple_extractor_plugin,
    simple_loader_plugin,
    simple_transform_load_plugin,
    simple_transform_plugin,
)


@pytest.fixture(autouse=True)
def setup_logging_level() -> None:
    os.environ["LOG_LEVEL"] = "info"
    setup_logger()


@pytest.fixture(autouse=True)
def plugin_registry_setup() -> Generator[None]:
    PluginRegistry._registry = {}  # Ensure a clean state before each test
    yield
    PluginRegistry._registry = {}  # Clean up after each test


def _pipeline_factory(default_config: dict[str, Any]) -> Callable[..., Pipeline]:
    # Factory function for creating pipelines
    def create_pipeline(**overrides: dict[str, Any]) -> Pipeline:
        config = default_config.copy()
        config.update(overrides)

        phases = {
            "extract": ExtractPhase.model_construct(steps=config.get("extract")),
            "transform": (
                TransformPhase.model_construct(steps=config.get("transform"))
                if config["type"] == "ETL" or config["type"] == "ETLT"
                else None
            ),
            "load": LoadPhase.model_construct(steps=config.get("load")),
            "transform_at_load": (
                TransformLoadPhase.model_construct(steps=config.get("transform_at_load"))
                if config["type"] == "ELT" or config["type"] == "ETLT"
                else None
            ),
        }

        # Include only-non empty values
        phases = {k: v for k, v in phases.items() if v}

        return Pipeline(
            name=config["name"],
            description=config.get("description", ""),
            type=config["type"],  # type: ignore[reportArgumentType]
            needs=config["needs"],
            phases=phases,  # type: ignore[reportArgumentType]
        )

    return create_pipeline


@pytest.fixture
def etl_pipeline_factory(request: pytest.FixtureRequest) -> Callable[..., Pipeline]:
    default_config = {
        "name": "ETL Pipeline",
        "type": "ETL",
        "extract": [PluginWrapper(id="mock_extractor", func=simple_extractor_plugin(delay=0))],
        "transform": [
            PluginWrapper(
                id="mock_transformer",
                func=simple_transform_plugin(delay=0),
            )
        ],
        "load": [PluginWrapper(id="mock_loader", func=simple_loader_plugin(delay=0))],
        "needs": None,
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    return _pipeline_factory(default_config=default_config)


@pytest.fixture
def elt_pipeline_factory(request: pytest.FixtureRequest) -> Callable[..., Pipeline]:
    default_config = {
        "name": "ELT Pipeline",
        "type": "ELT",
        "extract": [PluginWrapper(id="mock_extractor", func=simple_extractor_plugin(delay=0))],
        "load": [PluginWrapper(id="mock_loader", func=simple_loader_plugin(delay=0))],
        "transform_at_load": [
            PluginWrapper(
                id="mock_load_transformer",
                func=simple_transform_load_plugin(query="SELECT 1"),
            )
        ],
        "needs": None,
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    return _pipeline_factory(default_config=default_config)


@pytest.fixture
def etlt_pipeline_factory(request: pytest.FixtureRequest) -> Callable[..., Pipeline]:
    default_config = {
        "name": "ETLT Pipeline",
        "type": "ETLT",
        "extract": [PluginWrapper(id="mock_extractor", func=simple_extractor_plugin(delay=0))],
        "transform": [
            PluginWrapper(
                id="mock_transformer",
                func=simple_transform_plugin(delay=0),
            )
        ],
        "load": [PluginWrapper(id="mock_loader", func=simple_loader_plugin(delay=0))],
        "transform_at_load": [
            PluginWrapper(
                id="mock_load_transformer",
                func=simple_transform_load_plugin(query="SELECT 1"),
            )
        ],
        "needs": None,
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    return _pipeline_factory(default_config=default_config)
