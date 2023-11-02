import pytest
import asyncio
import yaml
from unittest.mock import patch
import core.parser as parser
from core.models import Job
# Tests Utils
from test_config import StageTypes



def test_serialize_yaml() -> None:

    YAML_STR = """
    jobs:
        job1:
            name: Name
            extract: []
            transform: []
            load: []
    """

    serialized_yaml = parser.deserialize_yaml(YAML_STR)

    expected_dict = {
        'jobs': {
            'job1': {'name': 'Name', 'extract': [], 'transform': [], 'load': []}
        }
    }
    assert serialized_yaml == expected_dict

class TestType:

    def extract(self) -> str:
        return 'extract'
    
    def store(self) -> str:
        return 'store'
    
    def transform(self, data) -> str:
        return 'transformed'

    def load(self, data) -> str:
        return 'loaded data'

@patch('core.parser.Factory.get')
def test_parse_etl_components_extract(mock):
    job_data = {
        'extract': [
            {
                'type': 'test_type'
            }
        ]
    }
    mock.return_value = TestType

    parsed_component = parser.parse_etl_components(job_data, StageTypes.EXTRACT)

    assert len(parsed_component) == 1
    assert parsed_component[0] == TestType

@patch('core.parser.Factory.get')
def test_parse_etl_components_transform(mock):
    
    job_data = {
        'transform': [
            {
                'type': 'test_type'
            }
        ]
    }
    mock.return_value = TestType

    parsed_component = parser.parse_etl_components(job_data, StageTypes.TRANSFORM)

    assert len(parsed_component) == 1
    assert parsed_component[0] == TestType

@patch('core.parser.Factory.get')
def test_parse_etl_components_load(mock):
    job_data = {
        'load': [
            {
                'type': 'test_type'
            }
        ]
    }
    mock.return_value = TestType

    parsed_component = parser.parse_etl_components(job_data, StageTypes.LOAD)

    assert len(parsed_component) == 1
    assert parsed_component[0] == TestType


@patch('core.parser.parse_etl_components')
def test_parse_single_job(mock):
    job_data = {
        'extract': [
            {
                'type': 'test_type'
            }
        ],
        'transform': [
            {
                 'type': 'test_type'
            }
        ],
        'load': [
            {
                'type': 'test_type'
            }
        ]
    }

    mock.return_value = [TestType]

    result = parser.parse_single_job(job_data)
    
    assert len(result) == 3
    assert len(result['extract']) == 1 and result['extract'][0] == TestType
    assert len(result['transform']) == 1 and result['transform'][0] == TestType
    assert len(result['load']) == 1 and result['load'][0] == TestType


@patch('core.parser.parse_single_job')
def test_parse_jobs(mock):
    job_data = {
        'job1': {
            'extract': [
                {
                    'type': 'test_type',
                    'otherarg1': 'valuearg1'
                }
            ],
            'transform': [
                {
                    'type': 'test_type',
                    'otherarg2': 'valuearg2'
                }
            ],
            'load': [
                {
                    'type': 'test_type',
                    'otherarg3': 'valuearg3'
                }
            ]
        }
    }

    mock.return_value = {
        'extract': [
            TestType
        ],
        'transform': [
            TestType
        ],
        'load': [
            TestType
        ]
    }

    result = parser.parse_jobs(job_data)

    
    print(result)

    assert result == [
        Job.model_construct(
            name='job1', 
            extract=[TestType], 
            transform=[TestType], 
            load=[TestType],
            needs=None
        )
    ]
    assert isinstance(result[0], Job)
