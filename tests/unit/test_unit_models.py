# Standard Imports

# Third-party Imports
import pytest

# Project Imports
import core.models.extract as extract
import core.models.load as load
import core.models.transform as tf
from core.models.phases import ExtractPhase, PipelinePhase
from core.models.pipeline import PipelineType
from plugins.registry import PluginFactory
from tests.common.constants import ELT, ETL, EXTRACT_PHASE, LOAD_PHASE, TRANSFORM_PHASE, LOAD_TRANSFORM_PHASE
from tests.common.mocks import MockExtractor, MockLoad, MockTransform, MockLoadTransform

@pytest.mark.asyncio
async def test_run_extractor(mock_extractor) -> None:
    result = await mock_extractor.extract_data()
    assert result == extract.ExtractResult(
        name="mock_extractor", success=True, result="extracted_data", error=None
    )


def test_run_transformer(mock_transformer) -> None:
    result = mock_transformer.transform_data("extracted_data")

    assert result == tf.TransformResult(
        name="mock_transformer",
        type=MockTransform,
        success=True,
        result="transformed_etl_data",
        error=None,
    )


def test_run_post_transformer(mock_load_transformer) -> None:
    result = mock_load_transformer.transform_data()

    assert result == tf.TransformResult(
        name="mock_load_transformer",
        type=MockLoadTransform,
        success=True,
        result=None,
        error=None
    )


@pytest.mark.asyncio
async def test_run_loader(mock_loader) -> None:
    result = await mock_loader.load_data("transformed_data")
    assert result == load.LoadResult(name="mock_loader", success=True, error=None)


def test_etl_pipeline_init(etl_pipeline_factory) -> None:
    pipeline = etl_pipeline_factory(name="Job1")

    assert pipeline.name == "Job1"
    assert pipeline.type == PipelineType.ETL
    assert pipeline.needs is None

    assert len(pipeline.phases[EXTRACT_PHASE].steps) == 1
    assert len(pipeline.phases[TRANSFORM_PHASE].steps) == 1
    assert len(pipeline.phases[LOAD_PHASE].steps) == 1


    assert isinstance(pipeline.phases[EXTRACT_PHASE].steps[0], extract.IExtractor)
    assert isinstance(pipeline.phases[TRANSFORM_PHASE].steps[0], tf.ITransform)
    assert isinstance(pipeline.phases[LOAD_PHASE].steps[0], load.ILoader)

    assert not pipeline.is_executed


def test_elt_pipeline_init(elt_pipeline_factory) -> None:
    pipeline = elt_pipeline_factory(name="ELT Pipeline")

    assert pipeline.name == "ELT Pipeline"
    assert pipeline.type == PipelineType.ELT
    assert pipeline.needs is None

    assert len(pipeline.phases[EXTRACT_PHASE].steps) == 1
    assert len(pipeline.phases[LOAD_PHASE].steps) == 1
    assert len(pipeline.phases[LOAD_TRANSFORM_PHASE].steps) == 1


    assert isinstance(pipeline.phases[EXTRACT_PHASE].steps[0], extract.IExtractor)
    assert isinstance(pipeline.phases[LOAD_PHASE].steps[0], load.ILoader)
    assert isinstance(pipeline.phases[LOAD_TRANSFORM_PHASE].steps[0], tf.ILoadTransform)

    assert not pipeline.is_executed


def test_elt_pipeline_init(etlt_pipeline_factory) -> None:
    pipeline = etlt_pipeline_factory()

    assert pipeline.name == "ETLT Pipeline"
    assert pipeline.type == PipelineType.ETLT
    assert pipeline.needs is None

    assert len(pipeline.phases[EXTRACT_PHASE].steps) == 1
    assert len(pipeline.phases[TRANSFORM_PHASE].steps) == 1
    assert len(pipeline.phases[LOAD_PHASE].steps) == 1
    assert len(pipeline.phases[LOAD_TRANSFORM_PHASE].steps) == 1


    assert isinstance(pipeline.phases[EXTRACT_PHASE].steps[0], extract.IExtractor)
    assert isinstance(pipeline.phases[TRANSFORM_PHASE].steps[0], tf.ITransform)
    assert isinstance(pipeline.phases[LOAD_PHASE].steps[0], load.ILoader)
    assert isinstance(pipeline.phases[LOAD_TRANSFORM_PHASE].steps[0], tf.ILoadTransform)

    assert not pipeline.is_executed