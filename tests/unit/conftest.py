# Standard Imports

# Third-party Imports
import pytest


# Project Imports
from core.models.phases import PipelinePhase
from core.models.pipeline import (
    ExtractPhase,
    LoadPhase,
    Pipeline,
    TransformLoadPhase,
    TransformPhase,
)
from tests.common.mocks import (
    MockExtractor, 
    MockLoad, 
    MockTransform, 
    MockLoadTransform
)


@pytest.fixture(scope="session")
def mock_extractor():
    return MockExtractor(id="mock_extractor", plugin="mock")
   

@pytest.fixture(scope="session")
def mock_loader():
    return MockLoad(id="mock_loader", plugin="mock")


@pytest.fixture(scope="session")
def mock_transformer():
    return MockTransform(id="mock_transformer")


@pytest.fixture(scope="session")
def mock_load_transformer():
    return MockLoadTransform(id="mock_load_transformer", query="SELECT 1")


@pytest.fixture(scope="session")
def extractor_plugin_data():
    return {
        "id": "extractor_id",
        "plugin": "mock_extractor",
    }


@pytest.fixture(scope="session")
def extractor_plugin_data_2():
    return {"id": "extractor_id_2", "plugin": "mock_extractor_2"}


@pytest.fixture(scope="session")
def loader_plugin_data():
    return {"id": "loader_id", "plugin": "mock_loader"}


@pytest.fixture(scope="session")
def loader_plugin_data_2():
    return {"id": "loader_id_2", "plugin": "mock_loader_2"}


@pytest.fixture(scope="session")
def transformer_plugin_data():
    return {"id": "transformer_id", "plugin": "mock_transformer"}


@pytest.fixture(scope="session")
def transformer_plugin_data_2():
    return {"id": "transformer_id2", "plugin": "mock_transformer_2"}


@pytest.fixture(scope="session")
def transform_at_loader_plugin_data():
    return {"id": "mock_transform_load_id", "query": "SELECT 1 FROM TABLE"}


def pipeline_factory(default_config):
    # Factory function for creating pipelines
    def create_pipeline(**overrides):
        config = default_config.copy()
        config.update(overrides)

        phases = [{
            "extract": ExtractPhase(steps=config.get("extract")),
            "transform": TransformPhase(steps=config.get("transform")) if config['type'] == "ETL" or config['type'] == "ETLT" else None,
            "load": LoadPhase(steps=config.get("load")),
            "transform_at_load": TransformLoadPhase(steps=config.get("transform_at_load")) if config['type'] == "ELT" or config['type'] == "ETLT" else None,
        }]

        # Include only-non empty values
        phases = [{k: v for k, v in phases[0].items() if v}]

        return Pipeline(
            name=config["name"],
            description=config.get("description", ""),
            type=config["type"],
            needs=None,
            phases=phases,
        )

    return create_pipeline


@pytest.fixture(scope="session")
def etl_pipeline_factory(request, mock_extractor, mock_transformer, mock_loader):
    default_config = {
        "name": "ETL Pipeline",
        "type": "ETL",
        "extract": [mock_extractor],
        "transform": [mock_transformer],
        "load": [mock_loader],
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline


@pytest.fixture(scope="session")
def elt_pipeline_factory(
    request, mock_extractor,  mock_loader, mock_load_transformer
):
    default_config = {
        "name": "ELT Pipeline",
        "type": "ELT",
        "extract": [mock_extractor],
        "load": [mock_loader],
        "transform_at_load": [mock_load_transformer],
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline

@pytest.fixture(scope="session")
def etlt_pipeline_factory(
    request, mock_extractor,  mock_transformer, mock_loader, mock_load_transformer
):
    default_config = {
        "name": "ETLT Pipeline",
        "type": "ETLT",
        "extract": [mock_extractor],
        "transform": [mock_transformer],
        "load": [mock_loader],
        "transform_at_load": [mock_load_transformer],
    }

    if hasattr(request, "param"):
        default_config.update(request.param)

    pipeline = pipeline_factory(default_config=default_config)
    return pipeline