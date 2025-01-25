# Standard ImportsCallable
from collections.abc import Callable

# Third-party Imports
# Project Imports
from core.models.pipeline import Pipeline, PipelineType
from core.plugins import PluginWrapper


def test_etl_pipeline_init_success(etl_pipeline_factory: Callable[..., Pipeline]) -> None:
    pipeline = etl_pipeline_factory(name="ETL Pipeline 1")

    assert pipeline.name == "ETL Pipeline 1"
    assert pipeline.type == PipelineType.ETL
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.transform.steps) == 1
    assert len(pipeline.load.steps) == 1

    assert isinstance(pipeline.extract.steps[0], PluginWrapper)
    assert isinstance(pipeline.transform.steps[0], PluginWrapper)
    assert isinstance(pipeline.load.steps[0], PluginWrapper)

    assert not pipeline.is_executed


def test_elt_pipeline_init_success(elt_pipeline_factory: Callable[..., Pipeline]) -> None:
    pipeline = elt_pipeline_factory(name="ELT Pipeline")

    assert pipeline.name == "ELT Pipeline"
    assert pipeline.type == PipelineType.ELT
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.load.steps) == 1
    assert len(pipeline.load_transform.steps) == 1

    assert isinstance(pipeline.extract.steps[0], PluginWrapper)
    assert isinstance(pipeline.load.steps[0], PluginWrapper)
    assert isinstance(pipeline.load_transform.steps[0], PluginWrapper)

    assert not pipeline.is_executed


def test_etlt_pipeline_init_success(etlt_pipeline_factory: Callable[..., Pipeline]) -> None:
    pipeline = etlt_pipeline_factory(name="ETLT Pipeline")

    assert pipeline.name == "ETLT Pipeline"
    assert pipeline.type == PipelineType.ETLT
    assert pipeline.needs is None

    assert len(pipeline.extract.steps) == 1
    assert len(pipeline.transform.steps) == 1
    assert len(pipeline.load.steps) == 1
    assert len(pipeline.load_transform.steps) == 1

    assert isinstance(pipeline.extract.steps[0], PluginWrapper)
    assert isinstance(pipeline.transform.steps[0], PluginWrapper)
    assert isinstance(pipeline.load.steps[0], PluginWrapper)
    assert isinstance(pipeline.load_transform.steps[0], PluginWrapper)

    assert not pipeline.is_executed
