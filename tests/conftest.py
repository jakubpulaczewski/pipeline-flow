# Standard Imports
import os
from collections.abc import Callable
from typing import Any, Generator

# Third-party Imports
import pytest

# Project Imports
from pipeline_flow.common.utils import setup_logger
from pipeline_flow.core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from pipeline_flow.core.models.pipeline import Pipeline
from pipeline_flow.core.registry import PluginRegistry
from tests.resources.plugins import (
    SimpleExtractorPlugin,
    SimpleLoaderPlugin,
    SimpleTransformLoadPlugin,
    SimpleTransformPlugin,
)


@pytest.fixture(autouse=True)
def setup_logging_level() -> None:
    os.environ["LOG_LEVEL"] = "info"
    setup_logger()


@pytest.fixture
def restart_plugin_registry() -> Generator[None]:
    # When running multiple tests, the PluginRegistry singleton will retain state between tests.
    # This fixture ensures that the PluginRegistry is reset before each test, such that
    # ValueError is not raised when registering the same plugin multiple times.
    PluginRegistry._registry = {}  # Ensure a clean state before each test
    yield


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
        "extract": [
            SimpleExtractorPlugin(plugin_id="mock_extractor"),
        ],
        "transform": [
            SimpleTransformPlugin(plugin_id="mock_transformer"),
        ],
        "load": [
            SimpleLoaderPlugin(plugin_id="mock_loader"),
        ],
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
        "extract": [
            SimpleExtractorPlugin(plugin_id="mock_extractor"),
        ],
        "load": [
            SimpleLoaderPlugin(plugin_id="mock_loader"),
        ],
        "transform_at_load": [
            SimpleTransformLoadPlugin(plugin_id="mock_load_transformer", query="SELECT 1"),
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
        "extract": [
            SimpleExtractorPlugin(plugin_id="mock_extractor"),
        ],
        "transform": [
            SimpleTransformPlugin(plugin_id="mock_transformer"),
        ],
        "load": [
            SimpleLoaderPlugin(plugin_id="mock_loader"),
        ],
        "transform_at_load": [
            SimpleTransformLoadPlugin(plugin_id="mock_load_transformer", query="SELECT 1"),
        ],
        "needs": None,
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    return _pipeline_factory(default_config=default_config)
