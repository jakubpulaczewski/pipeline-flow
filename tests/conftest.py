# Standard Imports
import os
from typing import Any, Callable, Generator

# Third-party Imports
import pytest

from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.models.pipeline import Pipeline
from core.plugins import PluginRegistry, PluginWrapper

# Project Imports
from tests.resources import mocks


@pytest.fixture(autouse=True)
def setup_logging_level() -> None:
    os.environ["LOG_LEVEL"] = "debug"


@pytest.fixture(autouse=True)
def plugin_registry_setup() -> Generator[None]:
    PluginRegistry._registry = {}  # Ensure a clean state before each test
    yield
    PluginRegistry._registry = {}  # Clean up after each test


def pipeline_factory(default_config: dict[str, Any]) -> Callable[..., Pipeline]:
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
            type=config["type"],
            needs=config["needs"],
            phases=phases,
        )

    return create_pipeline


@pytest.fixture
def etl_pipeline_factory(request: pytest.FixtureRequest) -> Callable[..., Pipeline]:
    default_config = {
        "name": "ETL Pipeline",
        "type": "ETL",
        "extract": [PluginWrapper(id="mock_extractor", func=mocks.mock_extractor(id="mock_extractor"))],
        "transform": [
            PluginWrapper(
                id="mock_transformer",
                func=mocks.mock_transformer(id="mock_transformer"),
            )
        ],
        "load": [PluginWrapper(id="mock_loader", func=mocks.mock_loader(id="mock_loader"))],
        "needs": None,
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    return pipeline_factory(default_config=default_config)


@pytest.fixture
def elt_pipeline_factory(request: pytest.FixtureRequest) -> Callable[..., Pipeline]:
    default_config = {
        "name": "ELT Pipeline",
        "type": "ELT",
        "extract": [PluginWrapper(id="mock_extractor", func=mocks.mock_extractor(id="mock_extractor"))],
        "load": [PluginWrapper(id="mock_loader", func=mocks.mock_loader(id="mock_loader"))],
        "transform_at_load": [
            PluginWrapper(
                id="mock_load_transformer",
                func=mocks.mock_load_transformer(id="mock_load_transformer", query="SELECT 1"),
            )
        ],
        "needs": None,
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    return pipeline_factory(default_config=default_config)


@pytest.fixture
def etlt_pipeline_factory(request: pytest.FixtureRequest) -> Callable[..., Pipeline]:
    default_config = {
        "name": "ETLT Pipeline",
        "type": "ETLT",
        "extract": [PluginWrapper(id="mock_extractor", func=mocks.mock_extractor(id="mock_extractor"))],
        "transform": [
            PluginWrapper(
                id="mock_transformer",
                func=mocks.mock_transformer(id="mock_transformer"),
            )
        ],
        "load": [PluginWrapper(id="mock_loader", func=mocks.mock_loader(id="mock_loader"))],
        "transform_at_load": [
            PluginWrapper(
                id="mock_load_transformer",
                func=mocks.mock_load_transformer(id="mock_load_transformer", query="SELECT 1"),
            )
        ],
        "needs": None,
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    return pipeline_factory(default_config=default_config)
