# Standard Imports
import tempfile
import os
import yaml

from unittest.mock import  patch, MagicMock
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
from core.models.phases import ExtractPhase, TransformPhase, TransformLoadPhase, LoadPhase
from plugins.registry import PluginRegistry
from tests.resources.constants import ETL, EXTRACT_PHASE, TRANSFORM_PHASE, LOAD_PHASE
from tests.resources.mocks import (
    MockExtractor, 
    MockLoad, 
    MockTransform, 
    MockLoadTransform, 
    MockMerger
)




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

    @pytest.fixture
    def mock_isdir(self, mocker) -> MagicMock:
        return mocker.patch("os.path.isdir", return_value=False)

    def test_get_all_files_with_empty_input(self, mock_isdir) -> None:
        plugin_parser = PluginParser(plugins_payload={})
        assert plugin_parser.get_all_files([]) == set()


    def test_get_all_files_with_no_valid_files(self, mock_isdir) -> None:
        paths = ["file1.txt", "file2.log"]
        plugin_parser = PluginParser(plugins_payload={})

        result = plugin_parser.get_all_files(paths)

        assert result == set()


    def test_get_all_files_with_only_files(self, mock_isdir):
        paths = ["file1.py", "file2.txt", "file3.py"]
        plugin_parser = PluginParser(plugins_payload={})

        result = plugin_parser.get_all_files(paths)
    
        assert result == {'file1.py', 'file3.py'}
    
    def test_get_all_files_with_only_duplicates(self, mock_isdir):
        paths = ["file1.py", "file3.py", "file3.py", "file3.py"]
        plugin_parser = PluginParser(plugins_payload={})

        result = plugin_parser.get_all_files(paths)
    
        assert result == {'file1.py', 'file3.py'}


    def test_get_all_files_with_directories_and_files(self, mocker):
        mock_isdir = mocker.patch("os.path.isdir")
        mock_listdir = mocker.patch("os.listdir")

        # Define mock behavior
        mock_isdir.side_effect = lambda path: path == "dir1"
        mock_listdir.return_value = ["file1.py", "file2.txt", "file3.py"]

        paths = ["dir1", "file4.py", "file5.txt"]
        plugin_parser = PluginParser(plugins_payload={})

        result = plugin_parser.get_all_files(paths)
        expected = {"dir1/file1.py", "dir1/file3.py", "file4.py"}
        assert result == expected
    
    
    def test_fetch_custom_plugin_files_with_no_dirs_or_files(self, mocker):        
        plugin_parser = PluginParser(plugins_payload={})

        mocker.patch.object(plugin_parser, "get_all_files", side_effect=[
            set(),
            set() 
        ])

        custom_plugins = plugin_parser.fetch_custom_plugin_files()

        assert custom_plugins == set()
        
    
    def test_fetch_custom_plugin_files_with_dirs_and_files(self, mocker):     
        plugin_parser = PluginParser(
            plugins_payload={
                "custom": {
                    "dirs": ["dir1"],
                    "files": ["file1.py", "file2.py"]     
                }  
            }
        )  
        files_mock = mocker.patch.object(plugin_parser, "get_all_files", side_effect=[
            {"dir1/fileA.py", "dir2/fileB.py"},  # Mocked output for directories
            {"file1.py", "file2.py"}             # Mocked output for files
        ])

        custom_plugins = plugin_parser.fetch_custom_plugin_files()

        files_mock.assert_any_call(['dir1'])
        files_mock.assert_any_call(["file1.py", "file2.py"])

        assert custom_plugins == {'file2.py', 'dir2/fileB.py', 'file1.py', 'dir1/fileA.py'}
        


    def test_fetch_custom_plugin_files_with_only_files(self, mocker):
        plugin_parser = PluginParser(
            plugins_payload={
                "custom": {
                    "files": ["file1.py", "file2.py"]     
                }  
            }
        )  

        files_mock = mocker.patch.object(plugin_parser, "get_all_files", side_effect=[
            set(),  # Mocked output for directories
            {"file1.py", "file2.py"}  # Mocked output for files
        ])

        custom_plugins = plugin_parser.fetch_custom_plugin_files()
        files_mock.assert_any_call(["file1.py", "file2.py"])

        assert custom_plugins == {'file2.py', 'file1.py'}

    
    def test_fetch_custom_plugin_files_with_overlapping_files(self, mocker):
        plugin_parser = PluginParser(
            plugins_payload={
                "custom": {
                    "dirs": ["dir1", "dir2"],
                    "files": ["dir1/fileA.py", "dir2/fileB.py"]     
                }  
            }
        )  

        files_mock = mocker.patch.object(plugin_parser, "get_all_files", side_effect=[
            {"dir1/fileA.py", "dir2/fileB.py"},
            {"dir1/fileA.py", "dir2/fileB.py"}
        ])

        custom_plugins = plugin_parser.fetch_custom_plugin_files()

        files_mock.assert_any_call(["dir1", "dir2"])
        files_mock.assert_any_call(["dir1/fileA.py", "dir2/fileB.py"]     )

        assert custom_plugins ==  {"dir1/fileA.py", "dir2/fileB.py"}

    def test_fetch_community_plugin_modules_empty(self) -> None:
        plugin_parser = PluginParser(plugins_payload={})

        assert plugin_parser.fetch_community_plugin_modules() == set()


    def test_fetch_community_plugin_modules_success(self) -> None:
        plugin_parser = PluginParser(
            plugins_payload={
                "community": [
                    "plugin1",
                    "plugin2",
                    "plugin3"

                ] 
            }
        ) 

        result = plugin_parser.fetch_community_plugin_modules()

        assert result == {"community.plugins.plugin1", "community.plugins.plugin2", "community.plugins.plugin3"}


class TestUnitPipelineParser:
    
    @pytest.fixture(autouse=True)
    def pipeline_parser(self) -> PipelineParser:
        self.pipeline_parser = PipelineParser()


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
                        extractor_plugin_data, 
                        second_extractor_plugin_data
                    ]
                },
                "transform": {
                    "steps": []
                },
                "load": {
                    "steps": [
                        loader_plugin_data, 
                        second_loader_plugin_data
                    ]
                }
            }
        }

        mocker.patch.object(
            self.pipeline_parser,
            "create_phase",
            side_effect=[
                ExtractPhase.model_construct(steps=[
                    MockExtractor(id='extractor_id'),
                    MockExtractor(id='extractor_id_2')
                ]),
                TransformPhase.model_construct(steps=[]),
                LoadPhase.model_construct(steps=[
                    MockLoad(id='loader_id'),
                    MockLoad(id='loader_id_2')
                ]),
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
            self.pipeline_parser,
            "create_phase",
            side_effect=[
                ExtractPhase.model_construct(steps=[
                    MockExtractor(id='extractor_id'),
                    MockExtractor(id='extractor_id_2')
                ]),
                TransformPhase.model_construct(steps=[
                    MockTransform(id='transformer_id'),
                    MockTransform(id='transformer_id_2')
                ]),
                LoadPhase.model_construct(steps=[
                    MockLoad(id='loader_id'),
                    MockLoad(id='loader_id_2')
                ]),
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

