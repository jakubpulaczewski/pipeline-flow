import yaml
from core.factory import Factory
from core.models import Job
from common.config import ETLStages

def deserialize_yaml(yaml_str: str) -> dict:
    """Deserialiazes a yaml string into python objects i.e. dicts, strings, int"""
    return yaml.safe_load(yaml_str)

def parse_etl_components(job_data: dict, stage_type: str):
    """Parse individual ETL components."""
    components = []
    for args in job_data.get(stage_type, []):
        service = args.pop('type')
        component = Factory.get(stage_type, service)
        components.append(component)
    return components

def parse_single_job(job_data: dict):
    """Parse a single job's data."""
    parsed_data = job_data.copy()
    for stage_type in [ETLStages.extract.name, ETLStages.transform.name, ETLStages.load.name]:
        parsed_data[stage_type] = parse_etl_components(job_data, stage_type)

    return parsed_data

def parse_jobs(jobs: dict):
    """Parse all jobs."""
    parsed_jobs = [
        Job(name=job_name, **parse_single_job(job_data)) for job_name, job_data in jobs.items()
    ]
    return parsed_jobs
