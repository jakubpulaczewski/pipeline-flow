# Standard Imports
from pathlib import Path
import shutil

# Third-party imports
import pytest

import tests.resources.mocks as mocks

# Project Imports
from core.parser import (
	YamlParser,
	PluginParser,
	PipelineParser
)

from core.models.pipeline import Pipeline
from core.plugins import PluginRegistry, PluginWrapper

from tests.resources.constants import (
	EXTRACT_PHASE, 
	TRANSFORM_PHASE,
	LOAD_PHASE,
	LOAD_TRANSFORM_PHASE
)
# from tests.resources.mocks import MockExtractor, MockTransform, MockLoad, MockLoadTransform

def setup_plugins(plugin_dict):
    for phase, plugins in plugin_dict.items():
        for plugin_name, plugin_callable in plugins:
            PluginRegistry.register(phase, plugin_name, plugin_callable)


@pytest.fixture
def create_temp_plugin_folder():
    parent_path = Path(__file__).parent.parent

    plugins_dir =  parent_path / "resources" / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    assert plugins_dir.is_dir()

    yield plugins_dir 

     # Cleanup
    if plugins_dir.exists():
         shutil.rmtree(plugins_dir, ignore_errors=True)
         

class TestIntegrationPluginParser:
    
    @staticmethod
    def test_fetch_custom_plugin_files_with_only_files() -> None:
        yaml_str = """
        plugins:
          custom:
            files:
              - tests/resources/plugins/custom_extractor.py
              - tests/resources/plugins/custom_loader.py
        """
        plugins_payload = YamlParser(yaml_text=yaml_str).get_plugins_dict()
        plugin_parser = PluginParser(plugins_payload)

        result = plugin_parser.fetch_custom_plugin_files()

        expected = {
             'tests/resources/plugins/custom_extractor.py',
             'tests/resources/plugins/custom_loader.py'
        }

        assert result == expected

    @staticmethod
    def test_fetch_custom_plugins_with_not_found_folder() -> None:
        # Create a temp folder
        yaml_str = """
        plugins:
          custom:
            dirs:
              - tests/resources/plugins_not_found
        """
        plugins_payload = YamlParser(yaml_text=yaml_str).get_plugins_dict()
        plugin_parser = PluginParser(plugins_payload)

        result = plugin_parser.fetch_custom_plugin_files()

        assert result == set()

    @staticmethod
    def test_fetch_custom_plugin_files_with_dirs_and_files(create_temp_plugin_folder) -> None:
        # Create plugin 
        file_path = create_temp_plugin_folder / "plugin123.py"
        file_path.write_text("")   
        assert file_path.is_file()

        yaml_str = """
        plugins:
          custom:
            dirs:
              - tests/resources/plugins
            files:
              - tests/resources/plugins/custom_plugins.py
        """
        plugins_payload = YamlParser(yaml_text=yaml_str).get_plugins_dict()
        plugin_parser = PluginParser(plugins_payload)

        result = plugin_parser.fetch_custom_plugin_files()

        expected = {
             'tests/resources/plugins/plugin123.py',
             'tests/resources/plugins/custom_plugins.py'
        }

        assert result == expected


    def test_get_custom_plugin_files_empty(self) -> None:
        plugin_parser = PluginParser(plugins_payload={})
        result = plugin_parser.fetch_custom_plugin_files()

        assert result == set()


class TestIntegrationPipelineParser:

    @pytest.fixture(autouse=True)
    def pipeline_parser(self) -> PipelineParser:
        self.pipeline_parser = PipelineParser()

    


    def test_parse_pipeline_without_registered_plugins(self):
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

        with pytest.raises(ValueError, match="Plugin class was not found for following plugin `mock_s3`."):
            pipelines_data = YamlParser(yaml_text=yaml_str).get_pipelines_dict()
            self.pipeline_parser.parse_pipelines(pipelines_data=pipelines_data)


    def test_parse_etl_pipeline_with_missing_extract_phase(self):
        # Register Plugins
        plugins = {
            TRANSFORM_PHASE: [("transform_plugin", mocks.mock_transformer)],
            LOAD_PHASE: [("load_plugin", mocks.mock_loader)],
        }
        setup_plugins(plugins)

        yaml_str = """
        pipelines:
          pipeline1:
            type: ETL
            phases:
              transform:
                steps:
                  - id: mock_tranformation1
                    plugin: transform_plugin
              load:
                steps:
                  - id: mock_load1
                    plugin: load_plugin
        """
        with pytest.raises(
            ValueError,
            match="Validation Error: The provided phases do not match the required phases for pipeline type 'PipelineType.ETL'. Missing phases: {<PipelinePhase.EXTRACT_PHASE: 'extract'>}.",
        ):
            pipelines_data = YamlParser(yaml_text=yaml_str).get_pipelines_dict()
            self.pipeline_parser.parse_pipelines(pipelines_data)


    def test_parse_etl_pipeline_with_extra_phases(self):
        # Register Plugins
        plugins = {
            EXTRACT_PHASE: [("extractor_plugin", mocks.mock_extractor)],
            TRANSFORM_PHASE: [("transform_plugin", mocks.mock_transformer)],
            LOAD_PHASE: [("load_plugin", mocks.mock_loader)],
            LOAD_TRANSFORM_PHASE: [("transform_at_load_plugin", mocks.mock_load_transformer)]
        }
        setup_plugins(plugins)

        yaml_str = """
        pipelines:
          pipeline1:
            type: ETL
            phases:
              extract:
                steps:
                  - id: mock_extract1
                    plugin: extractor_plugin
              transform:
                steps:
                  - id: mock_tranformation1
                    plugin: transform_plugin
              load:
                steps:
                  - id: mock_load1
                    plugin: load_plugin
              transform_at_load:
                steps:
                  - id: mock_transfor_at_load
                    plugin: transform_at_load_plugin
                    query: SELECT 13
        """
        with pytest.raises(
            ValueError,
            match="Extra phases: {<PipelinePhase.TRANSFORM_AT_LOAD_PHASE: 'transform_at_load'>}",
        ):
            pipelines_data = YamlParser(yaml_text=yaml_str).get_pipelines_dict()
            self.pipeline_parser.parse_pipelines(pipelines_data)
           
           
        
    def test_parse_etl_pipeline_with_only_mandatory_phases(self) -> None:
        # Register Plugins
        plugins = {
            EXTRACT_PHASE: [("extractor_plugin", mocks.mock_extractor)],
            LOAD_PHASE: [("load_plugin", mocks.mock_loader)],
        }
        setup_plugins(plugins)
            
        yaml_str = """
        pipelines:
          pipeline1:
            type: ETL
            phases:
              extract:
                steps:
                  - id: mock_extract1
                    plugin: extractor_plugin
              load:
                steps:
                  - id: mock_load1
                    plugin: load_plugin
        """
        pipelines_data = YamlParser(yaml_text=yaml_str).get_pipelines_dict()
        pipelines = self.pipeline_parser.parse_pipelines(pipelines_data)

        pipeline = pipelines[0]

        assert len(pipelines) == 1
        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == "pipeline1"

        assert len(pipeline.extract.steps) == 1
        assert pipeline.extract.steps[0] == PluginWrapper(id='mock_extract1', func=mocks.mock_extractor(id='mock_extract1'))

        assert len(pipeline.load.steps) == 1
        assert pipeline.load.steps[0] == PluginWrapper(id='mock_load1', func=mocks.mock_loader(id='mock_load1'))



    def test_parse_etl_multiple_pipelines(self) -> None:
        # Register Required Plugins
        plugins = {
            EXTRACT_PHASE: [
                ("extract_plugin1", mocks.mock_extractor),
            ],
            TRANSFORM_PHASE: [
                ("aggregate_sum_etl", mocks.mock_transformer)
            ],
            LOAD_PHASE: [
                ("load_plugin1", mocks.mock_loader),
                ("load_plugin2", mocks.mock_loader),
            ],
        }
        setup_plugins(plugins)

        yaml_str = """
        pipelines:
          pipeline1:
            type: ETL
            phases:
              extract:
                steps:
                  - id: mock_extract1
                    plugin: extract_plugin1
              transform:
                steps:
                  - id: mock_tranformation1
                    plugin: aggregate_sum_etl
              load:
                steps:
                  - id: mock_load1
                    plugin: load_plugin1
          pipeline2:
            type: ETL
            phases:
              extract:
                steps:
                  - id: mock_extract2
                    plugin: extract_plugin1
              load:
                steps:
                  - id: mock_load2
                    plugin: load_plugin2
        """

        pipelines_data = YamlParser(yaml_text=yaml_str).get_pipelines_dict()
        pipelines = self.pipeline_parser.parse_pipelines(pipelines_data)

        assert len(pipelines) == 2
        assert isinstance(pipelines[0], Pipeline)
        assert isinstance(pipelines[1], Pipeline)

        # Pipeline 1
        assert len(pipelines[0].extract.steps) == 1
        assert pipelines[0].extract.steps[0] == PluginWrapper(id='mock_extract1', func=mocks.mock_extractor(id='mock_extract1'))

        assert len(pipelines[0].transform.steps) == 1
        assert pipelines[0].transform.steps[0] == PluginWrapper(id='mock_tranformation1', func=mocks.mock_transformer(id='mock_tranformation1'))

        assert len(pipelines[0].load.steps) == 1
        assert pipelines[0].load.steps[0] == PluginWrapper(id='mock_load1', func=mocks.mock_loader(id='mock_load1'))

        # Pipeline 2
        assert len(pipelines[1].extract.steps) == 1
        assert pipelines[1].extract.steps[0] == PluginWrapper(id='mock_extract2', func=mocks.mock_extractor(id='mock_extract2'))

        assert len(pipelines[1].load.steps) == 1
        assert pipelines[1].load.steps[0] == PluginWrapper(id='mock_load2', func=mocks.mock_loader(id='mock_load2'))


    def test_parse_elt_pipeline(self) -> None:
        # Register Required Plugins
        plugins = {
            EXTRACT_PHASE: [
                ("extract_plugin1", mocks.mock_extractor),
            ],
            LOAD_PHASE: [
                ("load_plugin1",  mocks.mock_loader),
            ],        
            LOAD_TRANSFORM_PHASE: [
                ("upsert_transformation", mocks.mock_load_transformer)
            ]

        }
        setup_plugins(plugins)

        yaml_str = """
        pipelines:
          pipeline1:
            type: ELT
            phases:
              extract:
                steps: 
                  - id: mock_extract1
                    plugin: extract_plugin1
              load:
                steps:
                  - id: mock_load1
                    plugin: load_plugin1
              transform_at_load:
                steps:
                  - id: mock_load_transformer1
                    plugin: upsert_transformation
                    query: Select 2
        """

        pipelines_data = YamlParser(yaml_text=yaml_str).get_pipelines_dict()
        pipelines = self.pipeline_parser.parse_pipelines(pipelines_data)

        assert len(pipelines) == 1
        assert isinstance(pipelines[0], Pipeline)
        assert pipelines[0].name == "pipeline1"


        assert len(pipelines[0].extract.steps) == 1
        assert pipelines[0].extract.steps[0] == PluginWrapper(id='mock_extract1', func=mocks.mock_extractor(id='mock_extract1'))

        assert len(pipelines[0].load.steps) == 1
        assert pipelines[0].load.steps[0] == PluginWrapper(id='mock_load1', func=mocks.mock_loader(id='mock_load1'))


        assert len(pipelines[0].load_transform.steps) == 1
        assert pipelines[0].load_transform.steps[0] == PluginWrapper(id='mock_load_transformer1', func=mocks.mock_load_transformer(id='mock_load_transformer1', query='Select 2'))



    def test_parse_etlt_pipeline(self) -> None:
        # Setup Required Plugins
        plugins = {
            EXTRACT_PHASE: [
                ("extract_plugin1", mocks.mock_extractor),
            ],
            TRANSFORM_PHASE: [
                ("transform_plugin", mocks.mock_transformer)
            ],
            LOAD_PHASE: [
                ("load_plugin1", mocks.mock_loader),
            ],
            LOAD_TRANSFORM_PHASE: [
                ("upsert_transformation", mocks.mock_load_transformer)
            ]
        }

        setup_plugins(plugins)

        yaml_str = """
        pipelines:
          pipeline_ETLT:
            type: ETLT
            phases:
              extract:
                steps: 
                  - id: mock_extract1
                    plugin: extract_plugin1
              transform:
                steps:
                  - id: mock_tranformation1
                    plugin: transform_plugin
              load:
                steps:
                  - id: mock_load1
                    plugin: load_plugin1
              transform_at_load:
                steps:
                  - id: mock_load_transformer1
                    plugin: upsert_transformation
                    query: Select 1
        """

        pipelines_data = YamlParser(yaml_text=yaml_str).get_pipelines_dict()
        pipelines = self.pipeline_parser.parse_pipelines(pipelines_data)

        assert len(pipelines) == 1
        assert isinstance(pipelines[0], Pipeline)
        assert pipelines[0].name == "pipeline_ETLT"


        assert len(pipelines[0].extract.steps) == 1
        assert pipelines[0].extract.steps[0] == PluginWrapper(id='mock_extract1', func=mocks.mock_extractor(id='mock_extract1'))

        assert len(pipelines[0].transform.steps) == 1
        assert pipelines[0].transform.steps[0] == PluginWrapper(id='mock_tranformation1', func=mocks.mock_transformer(id='mock_tranformation1'))

        assert len(pipelines[0].load.steps) == 1
        assert pipelines[0].load.steps[0] == PluginWrapper(id='mock_load1', func=mocks.mock_loader(id='mock_load1'))


        assert len(pipelines[0].load_transform.steps) == 1
        assert pipelines[0].load_transform.steps[0] == PluginWrapper(id='mock_load_transformer1', func=mocks.mock_load_transformer(id='mock_load_transformer1', query='Select 1'))

