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


def test_etl_pipeline_init_success(etl_pipeline_factory) -> None:
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


def test_elt_pipeline_init_success(elt_pipeline_factory) -> None:
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


def test_elt_pipeline_init_success(etlt_pipeline_factory) -> None:
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