# Standard Imports

# Third-party Imports

# Project Imports
import tests.resources.mocks as mocks

from core.models.pipeline import PipelineType
from plugins.registry import PluginWrapper


def test_etl_pipeline_init_success(etl_pipeline_factory) -> None:
    pipeline = etl_pipeline_factory(
        name="ETL Pipeline 1",
        extract=[PluginWrapper(id='mock_extractor', func=mocks.mock_extractor(id='mock_extractor'))],
        transform=[PluginWrapper(id='mock_transformer', func=mocks.mock_transformer(id='mock_transformer'))],
        load=[PluginWrapper(id='mock_loader', func=mocks.mock_loader(id='mock_loader'))]
    )

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


def test_elt_pipeline_init_success(elt_pipeline_factory) -> None:
    pipeline = elt_pipeline_factory(
        name="ELT Pipeline",
        extract=[PluginWrapper(id='mock_extractor', func=mocks.mock_extractor(id='mock_extractor'))],
        load=[PluginWrapper(id='mock_loader', func=mocks.mock_loader(id='mock_loader'))],
        transform_at_load=[PluginWrapper(id='mock_load_transformer', func=mocks.mock_load_transformer(id='mock_load_transformer', query="SELECT 1"))]
    )

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


def test_etlt_pipeline_init_success(etlt_pipeline_factory) -> None:
    pipeline = etlt_pipeline_factory(
        name="ETLT Pipeline",
        extract=[PluginWrapper(id='mock_extractor', func=mocks.mock_extractor(id='mock_extractor'))],
        transform=[PluginWrapper(id='mock_transformer', func=mocks.mock_transformer(id='mock_transformer'))],
        load=[PluginWrapper(id='mock_loader', func=mocks.mock_loader(id='mock_loader'))],
        transform_at_load=[PluginWrapper(id='mock_load_transformer', func=mocks.mock_load_transformer(id='mock_load_transformer', query="SELECT 1"))]
    )

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
