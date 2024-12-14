# Standard Imports
import tempfile
import os
import yaml
import shutil

from unittest.mock import call, patch, MagicMock
from pathlib import Path

# Third-party Imports
import pytest

from pydantic import ValidationError

# Project Imports
from core.parser import (
    YamlParser,
    PipelineParser,
    PluginParser
)
from core.models.pipeline import Pipeline
from core.models.phases import PipelinePhase
from plugins.registry import PluginFactory
from tests.resources.constants import ETL, EXTRACT_PHASE, TRANSFORM_PHASE, LOAD_PHASE
from tests.resources.mocks import MockExtractor, MockLoad, MockTransform

from tests.resources.plugins.custom_loader import CustomLoader
from tests.resources.plugins.custom_extractor import CustomExtractor


@pytest.fixture
def mock_yaml_parser() -> MagicMock:
    return MagicMock(spec=YamlParser)



@pytest.fixture(scope='session')
def yaml_pipeline():
    yaml_str = """
    pipelines:
      pipeline1:
        type: ETL
        phases:
          extract:
            steps:
              - id: mock_extract1
                plugin: mock_s3
          transform:
            steps:
              - id: mock_tranformation1
                plugin: aggregate_sum
          load:
            steps:
              - id: mock_load1
                plugin: mock_s3
    """

    return yaml_str

@pytest.fixture(scope='session')
def temporary_yaml_file(yaml_pipeline):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml")

    yaml_content = yaml.safe_load(yaml_pipeline)

    with open(temp_file.name, "w") as f:
        yaml.dump(yaml_content, f)
    
    yield temp_file.name

    os.remove(temp_file.name)


class TestUnitYamlParser:

    @staticmethod
    def test_parse_empty_yaml():
        with pytest.raises(ValueError):
            YamlParser(yaml_text="")


    @staticmethod
    def test_parse_invalid_yaml():
        yaml_content = """
        key1: value1
        key2 value2  # Missing colon
        key3:
        - item1
        - item2:
        """
        
        with pytest.raises(yaml.YAMLError):
            YamlParser(yaml_text=yaml_content)


    @staticmethod
    def test_parse_yaml_file_not_found():
        with pytest.raises(FileNotFoundError, match="File not found: not_found.yaml"):
            YamlParser(file_path="not_found.yaml")


    @staticmethod
    def test_parse_yaml_text_success(yaml_pipeline) -> None:
        serialized_yaml = YamlParser(yaml_text=yaml_pipeline).parsed_yaml

        expected_dict = {
            "pipelines": {
                "pipeline1": {
                    "type": "ETL",
                    "phases": {
                        "extract": {
                            "steps": [{"id": "mock_extract1", "plugin": "mock_s3"}],
                        },
                        "transform": {
                            "steps": [
                                {
                                    "id": "mock_tranformation1",
                                    "plugin": "aggregate_sum",
                                }
                            ],
                        },
                        "load": {
                            "steps": [{"id": "mock_load1", "plugin": "mock_s3"}],
                        }
                    }
                }
            }
        }

        assert (
            serialized_yaml == expected_dict
        ), f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"

    @staticmethod
    def test_parse_yaml_file_success(temporary_yaml_file) -> None:
        serialized_yaml = YamlParser(file_path=temporary_yaml_file).parsed_yaml

        expected_dict = {
            "pipelines": {
                "pipeline1": {
                    "type": "ETL",
                    "phases": {
                        "extract": {
                            "steps": [{"id": "mock_extract1", "plugin": "mock_s3"}],
                        },
                        "transform": {
                            "steps": [
                                {
                                    "id": "mock_tranformation1",
                                    "plugin": "aggregate_sum",
                                }
                            ],
                        },
                        "load": {
                            "steps": [{"id": "mock_load1", "plugin": "mock_s3"}],
                        }
                    }
                }
            }
        }
        

        assert (
            serialized_yaml == expected_dict
        ), f"Deserialized YAML does not match the expected dictionary. Got: {serialized_yaml}"


class TestUnitPluginParser:


    @pytest.fixture(autouse=True)
    def plugin_parser(self, mock_yaml_parser) -> PluginParser:
        self.plugin_parser = PluginParser(mock_yaml_parser)

    @pytest.fixture
    def mock_isdir(self, mocker) -> MagicMock:
        return mocker.patch("os.path.isdir", return_value=False)

    def test_get_all_files_with_empty_input(self, mock_isdir) -> None:
        assert self.plugin_parser.get_all_files([]) == set()


    def test_get_all_files_with_no_valid_files(self, mock_isdir) -> None:
        paths = ["file1.txt", "file2.log"]
        result = self.plugin_parser.get_all_files(paths)

        assert result == set()


    def test_get_all_files_with_only_files(self, mock_isdir):
        paths = ["file1.py", "file2.txt", "file3.py"]
        result = self.plugin_parser.get_all_files(paths)
    
        assert result == {'file1.py', 'file3.py'}

    def test_get_all_files_with_directories_and_files(self, mocker):
        mock_isdir = mocker.patch("os.path.isdir")
        mock_listdir = mocker.patch("os.listdir")

        # Define mock behavior
        mock_isdir.side_effect = lambda path: path == "dir1"
        mock_listdir.return_value = ["file1.py", "file2.txt", "file3.py"]

        paths = ["dir1", "file4.py", "file5.txt"]
        result = self.plugin_parser.get_all_files(paths)

        expected = {"dir1/file1.py", "dir1/file3.py", "file4.py"}
        assert result == expected
    
    
    def test_get_custom_plugin_files_with_no_dirs_or_files(self, mocker):        
        mocker.patch.object(self.plugin_parser, "get_all_files", side_effect=[
            set(),
            set() 
        ])

        custom_plugins = self.plugin_parser.get_custom_plugin_files()

        assert custom_plugins == set()
        
    
    def test_get_custom_plugin_files_with_dirs_and_files(self, mocker):       
        mocker.patch.object(self.plugin_parser, "get_all_files", side_effect=[
            {"dir1/fileA.py", "dir2/fileB.py"},  # Mocked output for directories
            {"file1.py", "file2.py"}             # Mocked output for files
        ])

        custom_plugins = self.plugin_parser.get_custom_plugin_files()

        assert custom_plugins == {'file2.py', 'dir2/fileB.py', 'file1.py', 'dir1/fileA.py'}


    def test_get_custom_plugin_files_with_only_files(self, mocker):
        
        mocker.patch.object(self.plugin_parser, "get_all_files", side_effect=[
            set(),  # Mocked output for directories
            {"file1.py", "file2.py"}  # Mocked output for files
        ])

        custom_plugins = self.plugin_parser.get_custom_plugin_files()

        assert custom_plugins == {'file2.py', 'file1.py'}

    
    def test_get_custom_plugin_files_with_overlapping_files(self, mocker):
        
        mocker.patch.object(self.plugin_parser, "get_all_files", side_effect=[
            {"dir1/fileA.py", "dir2/fileB.py"},
            {"dir1/fileA.py", "dir2/fileB.py"}
        ])

        custom_plugins = self.plugin_parser.get_custom_plugin_files()

        assert custom_plugins ==  {"dir1/fileA.py", "dir2/fileB.py"}



class TestUnitPipelineParser:

    @pytest.fixture(autouse=True)
    def pipeline_parser(self, mock_yaml_parser) -> PipelineParser:
        self.pipeline_parser = PipelineParser(mock_yaml_parser)
    

    def test_parse_one_plugin(self, extractor_plugin_data):
        phase_data = {
            "steps": [extractor_plugin_data]
        }

        with patch.object(PluginFactory, "get", return_value=MockExtractor) as mock:
            result = self.pipeline_parser.parse_plugins_by_phase(EXTRACT_PHASE, phase_data["steps"])

            assert len(result) == 1
            assert result[0] == MockExtractor(
                id="extractor_id", plugin="mock_extractor", config=None
            )

            mock.assert_called_with(EXTRACT_PHASE, "mock_extractor")

    def test_parse_multiple_plugins(self, extractor_plugin_data, second_extractor_plugin_data):
        phase_data = {
            "steps": [
                extractor_plugin_data,
                second_extractor_plugin_data
            ]
        }

        with patch.object(
            PluginFactory, "get", side_effect=[MockExtractor, MockExtractor]
        ) as mock:
            result = self.pipeline_parser.parse_plugins_by_phase(EXTRACT_PHASE, phase_data["steps"])

            assert len(result) == 2
            assert result[0] == MockExtractor(
                id="extractor_id", config=None
            )
            assert result[1] == MockExtractor(
                id="extractor_id_2", config=None
            )

            assert call(EXTRACT_PHASE, "mock_extractor") in mock.mock_calls
            assert call(EXTRACT_PHASE, "mock_extractor_2") in mock.mock_calls


    def test_create_pipeline_with_no_pipeline_attributes(self):
        with pytest.raises(ValueError, match="Pipeline attributes are empty"):
            self.pipeline_parser.create_pipeline(pipeline_name="pipeline_2", pipeline_data={})

    def test_create_pipeline_with_only_mandatory_phases(
        self,
        mocker,
        extractor_plugin_data,
        second_extractor_plugin_data,
        loader_plugin_data,
        second_loader_plugin_data
    ):
        pipeline_data = {
            "type": "ETL",
            "phases": {
                "extract": {
                    "steps": [
                        extractor_plugin_data, second_extractor_plugin_data
                    ]
                },
                "transform": {
                    "steps": []
                },
                "load": {
                    "steps": [
                        loader_plugin_data, second_loader_plugin_data
                    ]
                }
            }
        }

        mocker.patch.object(
            PluginFactory,
            "get",
            side_effect=[
                MockExtractor,
                MockExtractor,
                MockLoad,
                MockLoad,
            ],
        )

        pipeline = self.pipeline_parser.create_pipeline("full_pipeline", pipeline_data)
        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == "full_pipeline"

        assert len(pipeline.extract.steps) == 2
        assert isinstance(pipeline.extract.steps[0], MockExtractor)
        assert isinstance(pipeline.extract.steps[1], MockExtractor)

        assert len(pipeline.transform.steps) == 0

        assert len(pipeline.load.steps) == 2
        assert isinstance(pipeline.load.steps[0], MockLoad)
        assert isinstance(pipeline.load.steps[1], MockLoad)


    def test_create_pipeline_without_mandatory_phase(
        self,
        mocker,
        loader_plugin_data,
        second_loader_plugin_data
    ):
        pipeline_data = {
            "type": "ETL",
            "phases": {
                "extract": {
                    "steps": []
                },
                "transform": {
                    "steps": []
                },
                "load": {
                    "steps": [
                        loader_plugin_data, second_loader_plugin_data
                    ]
                }
            }
        }

        mocker.patch.object(
            PluginFactory,
            "get",
            side_effect=[
                MockLoad,
                MockLoad,
            ],
        )

        with pytest.raises(ValidationError, match="Validation Failed! Mandatory phase 'PipelinePhase.EXTRACT_PHASE' cannot be empty or missing plugins."):
            self.pipeline_parser.create_pipeline("full_pipeline", pipeline_data)

    def test_create_pipeline_with_multiple_sources_destinations(
        self,
        mocker,
        extractor_plugin_data,
        second_extractor_plugin_data,
        transformer_plugin_data,
        second_transformer_plugin_data,
        loader_plugin_data,
        second_loader_plugin_data
        ):
        pipeline_data = {
            "type": "ETL",
            "phases": {
                "extract": {
                    "steps": [
                        extractor_plugin_data, second_extractor_plugin_data
                    ]
                },
                "transform": {
                    "steps": [
                        transformer_plugin_data, second_transformer_plugin_data
                    ]
                },
                "load": {
                    "steps": [
                        loader_plugin_data, second_loader_plugin_data
                    ]
                }
            }
        }

        mocker.patch.object(
            PluginFactory,
            "get",
            side_effect=[
                MockExtractor,
                MockExtractor,
                MockTransform,
                MockTransform,
                MockLoad,
                MockLoad,
            ],
        )

        pipeline = self.pipeline_parser.create_pipeline("full_pipeline", pipeline_data)
        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == "full_pipeline"

        assert len(pipeline.extract.steps) == 2
        assert isinstance(pipeline.extract.steps[0], MockExtractor)
        assert isinstance(pipeline.extract.steps[1], MockExtractor)

        assert len(pipeline.transform.steps) == 2
        assert isinstance(pipeline.transform.steps[0], MockTransform)
        assert isinstance(pipeline.transform.steps[1], MockTransform)

        assert len(pipeline.load.steps) == 2
        assert isinstance(pipeline.load.steps[0], MockLoad)
        assert isinstance(pipeline.load.steps[1], MockLoad)

