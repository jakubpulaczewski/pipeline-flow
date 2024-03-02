from typing import Type

import yaml

from common.config import ETL_CLASS, ETLStages
from core.factory import PluginFactory
from core.models import Job


def deserialize_yaml(yaml_str: str) -> dict:
    """Deserialiazes a yaml string into python objects i.e. dicts, strings, int"""
    return yaml.safe_load(yaml_str)


def parse_etl_plugins(job_data: dict, stage_type: str) -> list[Type[ETL_CLASS]]:
    """Parse individual ETL plugins by fetching their respective plugin class."""
    plugins = []

    for args in job_data.get(stage_type, []):
        # Loop through each type in the same ETL Stage.
        service = args.pop("type")
        plugin = PluginFactory.get(stage_type, service)
        plugins.append(plugin)
    return plugins


def parse_single_job(job_data: dict) -> dict[str, list[Type[ETL_CLASS]]]:
    """Parse a single job's data."""
    parsed_data = job_data.copy()
    for stage_type in [
        ETLStages.EXTRACT.name,
        ETLStages.TRANSFORM.name,
        ETLStages.LOAD.name,
    ]:
        parsed_data[stage_type] = parse_etl_plugins(job_data, stage_type)

    return parsed_data


def parse_jobs(jobs: dict) -> list[Job]:
    """Parse all jobs."""
    parsed_jobs = [
        Job(name=job_name, **parse_single_job(job_data))
        for job_name, job_data in jobs.items()
    ]
    return parsed_jobs
