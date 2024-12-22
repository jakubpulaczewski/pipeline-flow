# Standard Imports
from types import FunctionType

# Third-party Imports
import pytest
from pydantic import ValidationError

# Project Imports
from plugins.registry import PluginRegistry


from core.models.phases import ExtractPhase, TransformPhase, TransformLoadPhase, LoadPhase, iMerger, IExtractor, PipelinePhase
from tests.resources.mocks import (
    MockExtractor, 
    MockLoad, 
    MockTransform, 
    MockLoadTransform, 
    MockMerger,
    upper_case_and_suffix_transform
)

@pytest.fixture
def plugin_mock(mocker):
    return mocker.patch.object(PluginRegistry, 'get')

@pytest.fixture
def plugin_registry_mock(plugin_mock):
    def plugin_fetcher(phase_pipeline, plugin_name):
        if "mock_extractor" in plugin_name:
            return MockExtractor
        elif "mock_merger" in plugin_name:
            return MockMerger
    
    plugin_mock.side_effect = plugin_fetcher

    return plugin_mock


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


@pytest.mark.parametrize('phase_class', [
    (ExtractPhase),
    (LoadPhase),
    (TransformLoadPhase)
])
def test_create_phase_without_mandary_phase(phase_class) -> None:    
    with pytest.raises(ValidationError, match= "List should have at least 1 item after validation"):
        phase_class(steps=[])


def test_create_phase_extract(plugin_mock, extractor_plugin_data) -> None:
    plugin_mock.return_value = MockExtractor

    extract = ExtractPhase(steps=[extractor_plugin_data])
    
    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.EXTRACT_PHASE, 'mock_extractor')
    assert extract == ExtractPhase.model_construct(steps=[MockExtractor(id='extractor_id')])



def test_create_phase_extract_with_merge_success(plugin_registry_mock, extractor_plugin_data, second_extractor_plugin_data, merger_plugin_data) -> None:   
    extract = ExtractPhase(
        steps=[extractor_plugin_data, second_extractor_plugin_data],
        merge = merger_plugin_data
    )

    # Behaviour check of the mock
    assert len(plugin_registry_mock.mock_calls) == 3
    plugin_registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, 'mock_extractor')
    plugin_registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, 'mock_extractor_2')
    plugin_registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, 'mock_merger')


    assert isinstance(extract.merge, iMerger)
    assert isinstance(extract.steps[0], IExtractor)
    assert isinstance(extract.steps[1], IExtractor)

    assert extract == ExtractPhase.model_construct(
        steps=[MockExtractor(id='extractor_id'), MockExtractor(id='extractor_id_2')], 
        merge=MockMerger()
    )


def test_create_phase_extract_with_merge_failure(plugin_registry_mock, extractor_plugin_data, merger_plugin_data) -> None:

    with pytest.raises(ValidationError, match="Validation Error! Merge can only be used if there is more than one extract step."):
        ExtractPhase(
            steps=[extractor_plugin_data],
            merge = merger_plugin_data
        )


def test_create_phase_with_same_id_failure(plugin_registry_mock) -> None:
    with pytest.raises(ValueError, match="The `ID` is not unique. There already exists an 'ID' with this name"):
        ExtractPhase(
            steps=[
                {
                    "id": "extractor_id",
                    "plugin": "mock_extractor",
                },
                {
                    "id": "extractor_id", 
                    "plugin": "mock_extractor_2"
                }
        ])


def test_create_phase_transform_without_steps_success():
    transform = TransformPhase(steps=[])

    assert transform == TransformPhase.model_construct(steps=[])


def test_create_phase_class_transform(plugin_mock, transformer_plugin_data) -> None:
    plugin_mock.return_value = MockTransform

    transform = TransformPhase(
        steps=[transformer_plugin_data]
    )

    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.TRANSFORM_PHASE, 'mock_transformer')
    assert transform == TransformPhase.model_construct(steps=[MockTransform(id='transformer_id')])


def test_create_phase_function_transform(plugin_mock, transformer_plugin_data) -> None:
    plugin_mock.return_value = upper_case_and_suffix_transform
    
    transform = TransformPhase(
        steps=[transformer_plugin_data]
    )
    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.TRANSFORM_PHASE, 'mock_transformer')

    assert isinstance(transform.steps[0], FunctionType)



def test_create_phase_load(plugin_mock, loader_plugin_data) -> None:
    plugin_mock.return_value = MockLoad

    loader = LoadPhase(
        steps=[loader_plugin_data]
    )

    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.LOAD_PHASE, 'mock_loader')

    assert loader == LoadPhase.model_construct(steps=[MockLoad(id='loader_id')])


def test_create_phase_transform_at_load(plugin_mock, transform_at_load_plugin_data) -> None:
    plugin_mock.return_value = MockLoadTransform

    result = TransformLoadPhase(
        steps=[transform_at_load_plugin_data]
    )

    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.TRANSFORM_AT_LOAD_PHASE, 'mock_transformer_loader')

    assert result == TransformLoadPhase.model_construct(steps=[MockLoadTransform(id='mock_transform_load_id')])
