# Standard Imports


# Third-party Imports
import pytest

# Project Imports
import core.models.extract as extract
import core.models.transform as tf
import core.models.load as load

from core.models.pipeline import Pipeline
from common.config import PipelineTypes
from common.type_def import (
    ExtractedData,
    TransformedData,
    LoadedData,
)


class MockExtractor(extract.IExtractor):

    @extract.extract_decorator
    async def extract_data(self) -> ExtractedData:
        return 'extracted_data'


class MockTransformETL(tf.ITransformerETL):

    @tf.transform_decorator
    def transform_data(self, data: ExtractedData) -> TransformedData:
       return 'transformed_etl_data'


class MockTransformELT(tf.ITransformerELT):

    @tf.transform_decorator
    def transform_data(self) -> None:
       return


class MockLoad(load.ILoader):

    @load.load_decorator
    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        return


@pytest.fixture(scope="session")
def mock_extractor():
    return MockExtractor(
        id="mock_extractor",
        type="mock"
    )


@pytest.fixture(scope="session")
def mock_loader():
    return MockLoad(
        id="mock_loader",
        type="mock"
    )


@pytest.fixture(scope="session")
def mock_transformer_etl():
    return MockTransformETL(
        id="mock_transformer_etl"
    )


@pytest.fixture(scope="session")
def mock_transformer_elt():
    return MockTransformELT(
        id="mock_transformer_elt",
        query="SELECT 1 FROM TABLE"
    )


@pytest.fixture(scope="session")
def elt_pipeline(mock_extractor, mock_transformer_elt, mock_loader) -> Pipeline:
    return Pipeline(
        name="Test ELT Pipeline",
        description="A test ELT Pipeline",
        type='ELT',
        needs=None,
        extract= extract.ExtractPhase(
            steps=[mock_extractor],
            storage=None
        ),
        transform=tf.TransformPhase(
            steps=[mock_transformer_elt],
            storage=None
        ),
        load=load.LoadPhase(
            steps=[mock_loader],
            storage=None
        )
    )


@pytest.fixture(scope="session")
def etl_pipeline(mock_extractor, mock_transformer_etl, mock_loader) -> Pipeline:

    return Pipeline(
        name="TEST ETL Pipeline",
        description="A test ETL Pipeline",
        type='ETL',
        needs=None,
        extract= extract.ExtractPhase(
            steps=[mock_extractor],
            storage=None
        ),
        transform=tf.TransformPhase(
            steps=[mock_transformer_etl],
            storage=None
        ),
        load=load.LoadPhase(
            steps=[mock_loader],
            storage=None
        )
    )


@pytest.mark.asyncio
async def test_run_extractor(mock_extractor) -> None:
    result = await mock_extractor.extract_data()
    assert result == extract.ExtractResult(name='mock_extractor', success=True, result='extracted_data', error=None)


def test_run_transformer_etl(mock_transformer_etl) -> None:
    result = mock_transformer_etl.transform_data('extracted_data')

    assert result == tf.TransformResult(name='mock_transformer_etl', type='ETL', success=True, result='transformed_etl_data', error=None)


def test_run_transformer_elt(mock_transformer_elt) -> None:
    result = mock_transformer_elt.transform_data()

    assert result == tf.TransformResult(name='mock_transformer_elt', type='ELT', success=True, error=None)



@pytest.mark.asyncio
async def test_run_loader(mock_loader) -> None:
    result = await mock_loader.load_data('transformed_data')
    assert result == load.LoadResult(name='mock_loader', success=True, error=None)


def test_etl_pipeline_init(etl_pipeline) -> None:
    assert etl_pipeline.name == "TEST ETL Pipeline"
    assert etl_pipeline.description == "A test ETL Pipeline"

    assert etl_pipeline.type in PipelineTypes.ALLOWED_PIPELINE_TYPES
    assert etl_pipeline.needs is None
    assert len([etl_pipeline.extract.steps]) == 1 and isinstance(etl_pipeline.extract.steps[0], extract.IExtractor)
    assert len(etl_pipeline.transform.steps) == 1 and isinstance(etl_pipeline.transform.steps[0], tf.ITransformerETL)
    assert len(etl_pipeline.load.steps) == 1 and isinstance(etl_pipeline.load.steps[0], load.ILoader)
    assert not etl_pipeline.is_executed


def test_elt_pipeline_init(elt_pipeline) -> None:
    assert elt_pipeline.name == "Test ELT Pipeline"
    assert elt_pipeline.description == "A test ELT Pipeline"

    assert elt_pipeline.type in PipelineTypes.ALLOWED_PIPELINE_TYPES
    assert elt_pipeline.needs is None
    assert len([elt_pipeline.extract.steps]) == 1 and isinstance(elt_pipeline.extract.steps[0], extract.IExtractor)
    assert len(elt_pipeline.transform.steps) == 1 and isinstance(elt_pipeline.transform.steps[0], tf.ITransformerELT)
    assert len(elt_pipeline.load.steps) == 1 and isinstance(elt_pipeline.load.steps[0], load.ILoader)
    assert not elt_pipeline.is_executed


def test_pipeline_type_validation(mock_extractor, mock_transformer_etl, mock_loader) -> None:
    with pytest.raises(ValueError):
        Pipeline(
            name="InvalidTypePipeline",
            type="INVALID",
            extract= extract.ExtractPhase(
                steps=[mock_extractor],
                storage=None
            ),
            transform=tf.TransformPhase(
                steps=[mock_transformer_etl],
                storage=None
            ),
            load=load.LoadPhase(
                steps=[mock_loader],
                storage=None
            )
        )
