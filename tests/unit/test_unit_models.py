# Standard Imports

# Third-party Imports
import pytest

# Project Imports
from core.models.phases import (
    IExtractor,
    ITransform,
    ILoader,
    ILoadTransform
)

from core.models.pipeline import PipelineType
from plugins.registry import PluginRegistry
from tests.resources.mocks import MockExtractor, MockLoad, MockTransform, MockLoadTransform

@pytest.mark.asyncio
async def test_run_extract_data(extractor_mock) -> None:
    result = await extractor_mock.extract_data()
    assert result == "extracted_data"

def test_run_transform_data(mock_transformer) -> None:
    result = mock_transformer.transform_data("extracted_data")

    assert result == "transformed_etl_data"


def test_run_transform_load_data(mock_load_transformer) -> None:
    result = mock_load_transformer.transform_data()

    assert result == None


@pytest.mark.asyncio
async def test_run_load_data(mock_loader) -> None:
    result = await mock_loader.load_data("transformed_data")
    assert result == None


def test_etl_pipeline_init(etl_pipeline_factory) -> None:
    pipeline = etl_pipeline_factory(name="Job1")

    assert pipeline.name == "Job1"
    assert pipeline.type == PipelineType.ETL
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.transform.steps) == 1
    assert len(pipeline.load.steps) == 1


    assert isinstance(pipeline.extract.steps[0], IExtractor)
    assert isinstance(pipeline.transform.steps[0], ITransform)
    assert isinstance(pipeline.load.steps[0], ILoader)

    assert not pipeline.is_executed


def test_elt_pipeline_init(elt_pipeline_factory) -> None:
    pipeline = elt_pipeline_factory(name="ELT Pipeline")

    assert pipeline.name == "ELT Pipeline"
    assert pipeline.type == PipelineType.ELT
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.load.steps) == 1
    assert len(pipeline.load_transform.steps) == 1


    assert isinstance(pipeline.extract.steps[0], IExtractor)
    assert isinstance(pipeline.load.steps[0], ILoader)
    assert isinstance(pipeline.load_transform.steps[0], ILoadTransform)

    assert not pipeline.is_executed


def test_elt_pipeline_init(etlt_pipeline_factory) -> None:
    pipeline = etlt_pipeline_factory()

    assert pipeline.name == "ETLT Pipeline"
    assert pipeline.type == PipelineType.ETLT
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.transform.steps) == 1
    assert len(pipeline.load.steps) == 1
    assert len(pipeline.load_transform.steps) == 1


    assert isinstance(pipeline.extract.steps[0], IExtractor)
    assert isinstance(pipeline.transform.steps[0], ITransform)
    assert isinstance(pipeline.load.steps[0], ILoader)
    assert isinstance(pipeline.load_transform.steps[0], ILoadTransform)

    assert not pipeline.is_executed