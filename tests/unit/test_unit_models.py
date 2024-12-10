# Standard Imports

# Third-party Imports
import pytest

# Project Imports
import core.models.extract as extract
import core.models.load as load
import core.models.transform as tf
from core.models.pipeline import PipelineType
from plugins.registry import PluginFactory
from tests.common.mocks import MockExtractor, MockLoad, MockTransform, MockLoadTransform

@pytest.mark.asyncio
async def test_run_extract_data(extractor_mock) -> None:
    result = await extractor_mock.extract_data()
    assert result == extract.ExtractResult(
        name="extractor_id", success=True, result="extracted_data", error=None
    )


def test_run_transform_data(mock_transformer) -> None:
    result = mock_transformer.transform_data("extracted_data")

    assert result == tf.TransformResult(
        name="transformer_id",
        type=MockTransform,
        success=True,
        result="transformed_etl_data",
        error=None,
    )


def test_run_transform_load_data(mock_load_transformer) -> None:
    result = mock_load_transformer.transform_data()

    assert result == tf.TransformResult(
        name="mock_transform_load_id",
        type=MockLoadTransform,
        success=True,
        result=None,
        error=None
    )


@pytest.mark.asyncio
async def test_run_load_data(mock_loader) -> None:
    result = await mock_loader.load_data("transformed_data")
    assert result == load.LoadResult(name="loader_id", success=True, error=None)


def test_etl_pipeline_init(etl_pipeline_factory) -> None:
    pipeline = etl_pipeline_factory(name="Job1")

    assert pipeline.name == "Job1"
    assert pipeline.type == PipelineType.ETL
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.transform.steps) == 1
    assert len(pipeline.load.steps) == 1


    assert isinstance(pipeline.extract.steps[0], extract.IExtractor)
    assert isinstance(pipeline.transform.steps[0], tf.ITransform)
    assert isinstance(pipeline.load.steps[0], load.ILoader)

    assert not pipeline.is_executed


def test_elt_pipeline_init(elt_pipeline_factory) -> None:
    pipeline = elt_pipeline_factory(name="ELT Pipeline")

    assert pipeline.name == "ELT Pipeline"
    assert pipeline.type == PipelineType.ELT
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.load.steps) == 1
    assert len(pipeline.load_transform.steps) == 1


    assert isinstance(pipeline.extract.steps[0], extract.IExtractor)
    assert isinstance(pipeline.load.steps[0], load.ILoader)
    assert isinstance(pipeline.load_transform.steps[0], tf.ILoadTransform)

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


    assert isinstance(pipeline.extract.steps[0], extract.IExtractor)
    assert isinstance(pipeline.transform.steps[0], tf.ITransform)
    assert isinstance(pipeline.load.steps[0], load.ILoader)
    assert isinstance(pipeline.load_transform.steps[0], tf.ILoadTransform)

    assert not pipeline.is_executed