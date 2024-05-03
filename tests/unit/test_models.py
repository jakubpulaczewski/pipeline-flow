# Standard Imports
from typing import TypeVar

# Third-party Imports
import pytest

# Project Imports
import core.models as models

T = TypeVar('T')

class MockExtract(models.ExtractInterface):
    def extract_data(self, enable_data_flow: bool) -> T:
        return "extracted_data"

class MockTransform(models.TransformInterface):
    def transform_data(self, data: T, enable_data_flow: bool) -> T:
       return "transformed_data"

class MockLoad(models.LoadInterface):
    def load_data(self, data: T) -> bool:
        return True
    
@pytest.fixture(params=["ETL", "ELT"])
def pipeline_instance(request):
    return models.Pipeline(
        name="TestPipeline",
        description="A test pipeline",
        type=request.param,
        needs=None,
        extract=[MockExtract()],
        transform=[MockTransform()],
        load=[MockLoad()],
    )

def test_pipeline_initialization(pipeline_instance):
    assert pipeline_instance.name == "TestPipeline"
    assert pipeline_instance.description == "A test pipeline"
    assert pipeline_instance.type in models.PipelineTypes.ALLOWED_PIPELINE_TYPES
    assert pipeline_instance.needs is None
    assert len(pipeline_instance.extract) == 1
    assert len(pipeline_instance.transform) == 1
    assert len(pipeline_instance.load) == 1
    assert not pipeline_instance.is_executed

def test_pipeline_type_validation():
    with pytest.raises(ValueError):
        models.Pipeline(
            name="InvalidTypePipeline",
            type="INVALID",
            extract=[MockExtract()],
            transform=[MockTransform()],
            load=[MockLoad()],
        )

def test_run_extractor(pipeline_instance):
    extracted_data, data_flow_strategy = pipeline_instance.run_extractor(pipeline_instance.extract)
    assert extracted_data == "extracted_data"
    assert data_flow_strategy == "direct"

def test_run_transformer(pipeline_instance):
    transformed_data, data_flow_strategy = pipeline_instance.run_transformer("input_data")
    assert transformed_data == "transformed_data"
    assert data_flow_strategy == "direct"

def test_run_loader(pipeline_instance):
    assert pipeline_instance.run_loader("transformed_data")
