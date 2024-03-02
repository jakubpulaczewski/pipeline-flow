# Standard Imports
from unittest.mock import patch

# Third-party Imports
import pytest
import test_utils as utils  # pylint: disable=import-error
from faker import Faker
from pytest_cases import parametrize_with_cases

# Project Imports
import common.config as config  # pylint: disable=consider-using-from-import
import core.parser as parser  # pylint: disable=consider-using-from-import
from core.models import Job

fake = Faker()


def mock_parse_etl_components(job_data: dict, stage_type: str):
    """Function to be used as side_effect for the mock"""
    return [utils.create_fake_class(plugin["type"]) for plugin in job_data[stage_type]]


@pytest.mark.parametrize("etl_stage", config.ETL_STAGES)
def case_etl_plugins(etl_stage):
    """A util function to generate a Plugin name and return a class associated with that plugin."""
    plugin = utils.generate_plugin_name()
    plugin_cls = utils.create_fake_class(plugin)

    job_data = {etl_stage: [{"type": plugin}]}

    return etl_stage, job_data, plugin_cls


def test_serialize_yaml() -> None:
    """A test that verifies the serialization of the YAML."""
    yaml_str = """
    jobs:
        job1:
            name: Name
            extract: []
            transform: []
            load: []
    """

    serialized_yaml = parser.deserialize_yaml(yaml_str)

    expected_dict = {
        "jobs": {"job1": {"name": "Name", "extract": [], "transform": [], "load": []}}
    }
    assert serialized_yaml == expected_dict


@patch("core.parser.PluginFactory.get")
@parametrize_with_cases("etl_stage, job_data, plugin_cls", cases=".")
def test_parse_etl_plugins(factory_mock, etl_stage, job_data, plugin_cls):
    """A test checking that plugins are parsed appropriately."""
    factory_mock.return_value = plugin_cls
    parsed_plugins = parser.parse_etl_plugins(job_data, etl_stage)

    assert len(parsed_plugins) == 1
    assert parsed_plugins[0] == plugin_cls


@patch("core.parser.parse_etl_plugins", side_effect=mock_parse_etl_components)
def test_parse_single_job(parse_mock):  # pylint: disable=unused-argument
    """A test that parses a single."""
    job_data = {
        "extract": [{"type": utils.generate_plugin_name()}],
        "transform": [
            {"type": utils.generate_plugin_name()},
            {"type": utils.generate_plugin_name()},
        ],
        "load": [{"type": utils.generate_plugin_name()}],
    }

    result = parser.parse_single_job(job_data)

    # Validates if the number of parsed service is right
    assert len(result) == 3
    assert len(result["extract"]) == 1
    assert len(result["transform"]) == 2
    assert len(result["load"]) == 1

    # Validates if they are all classes - can't check for typing since Protocols are used.
    assert isinstance(result["extract"][0], type)
    assert isinstance(result["transform"][0], type) and isinstance(
        result["transform"][1], type
    )
    assert isinstance(result["load"][0], type)


@patch("core.parser.parse_single_job")
def test_parse_jobs(mock):
    """A test parsing multiple jobs."""
    plugins = (
        utils.generate_plugin_name(),
        utils.generate_plugin_name(),
        utils.generate_plugin_name(),
    )
    plugin_cls = (
        utils.create_fake_class(plugins[0]),
        utils.create_fake_class(plugins[1]),
        utils.create_fake_class(plugins[2]),
    )

    job_data = {
        "job1": {
            "extract": [{"type": plugins[0], "otherarg1": "valuearg1"}],
            "transform": [{"type": plugins[1], "otherarg2": "valuearg2"}],
            "load": [{"type": plugins[2], "otherarg3": "valuearg3"}],
        }
    }
    mock.return_value = {
        "extract": [plugin_cls[0]],
        "transform": [plugin_cls[1]],
        "load": [plugin_cls[2]],
    }
    result = parser.parse_jobs(job_data)

    assert result == [
        Job.model_construct(
            name="job1",
            extract=[plugin_cls[0]],
            transform=[plugin_cls[1]],
            load=[plugin_cls[2]],
            needs=None,
        )
    ]
    assert isinstance(result[0], Job)
