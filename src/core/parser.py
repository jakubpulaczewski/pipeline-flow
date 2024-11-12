# Standard Imports

# Third-party imports
import yaml

# Project Imports
from common.config import ETLConfig
from common.type_def import ETL_PHASE_CALLABLE
from core.models.pipeline import Pipeline
from core.plugins import PluginFactory

PLUGIN_MANDATORY_FLAGS_BY_PHASE = {
    ETLConfig.EXTRACT_PHASE: True,
    ETLConfig.TRANSFORM_PHASE: False,
    ETLConfig.LOAD_PHASE: True,
}


def deserialize_yaml(yaml_str: str) -> dict:
    """Deserialiazes a yaml string into python objects i.e. dicts, strings, int"""
    if not yaml_str:
        raise ValueError("A YAML string you provided is empty.")
    return yaml.safe_load(yaml_str)


def is_mandatory_phase_empty(phase: str) -> bool:
    """Check if a phase is mandatory based on configuration."""
    return PLUGIN_MANDATORY_FLAGS_BY_PHASE.get(phase, False)


def validate_phase_configuration(phase: str, phase_args: dict) -> bool:
    """Validate if the given phase has mandatory plugins (cannot be empty) and
    if it is correctly configured.

    Args:
        phase (str): The phase of the ETL (e.g., extract, transform, load)
        phase_args (dict): Dict containing arguments for each phase.

    Raises:
        ValueError: True, if the phase passes validation.

    Returns:
        bool: If a mandatory phase is empty.
    """
    if is_mandatory_phase_empty(phase):
        # Check if "steps" exist and are not empty

        if not phase_args.get(ETLConfig.STEPS_KEY, []):
            raise ValueError(
                "Validation Failed! Mandatory phase '%s' cannot be empty or missing plugins.",
                phase,
            )

    return True


def parse_phase_steps_plugins(phase: str, phase_args: dict) -> list[ETL_PHASE_CALLABLE]:
    """Retrieves all the plugin objects.

    Args:
        phase (str): The phase of the ETL (e.g., extract, transform, load)
        phase_args (dict): Dict containing arguments for each phase, in the YAML.

    Returns:
        list[ETL_PHASE_CALLABLE]: A list of plugins for a given phase.
    """

    # Validation Step
    validate_phase_configuration(phase, phase_args)

    plugins = []
    for step in phase_args[ETLConfig.STEPS_KEY]:
        plugin_name = step.get('type')
        plugin =  PluginFactory.get(phase, plugin_name)(**step)
        plugins.append(plugin)
    
    return plugins


def create_pipeline_from_data(pipeline_name: str, pipeline_data: dict) -> Pipeline:
    """Parse a single pipeline's data and return a pipeline instance."""
    if not pipeline_data:
        raise ValueError("Pipeline attributes are empty")
    
    # Fetch the pipeline type (e.g., ETL, ELT, etc.)
    pipeline_type = pipeline_data.get('type')

    for phase in ETLConfig.get_pipeline_phases(pipeline_type):
        phase_args = pipeline_data.get(phase)
        pipeline_data[phase][ETLConfig.STEPS_KEY] = parse_phase_steps_plugins(
            phase, phase_args
        )

    return Pipeline(name=pipeline_name, **pipeline_data)


def create_pipelines_from_dict(pipelines: dict[str, dict]) -> list[Pipeline]:
    """Parse all pipelines and return a list of Pipeline instances."""
    return [
        create_pipeline_from_data(pipeline_name, pipeline_data)
        for pipeline_name, pipeline_data in pipelines.items()
    ]
