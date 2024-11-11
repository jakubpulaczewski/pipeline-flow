# Standard Imports
from __future__ import annotations

from enum import Enum

# Project Imports
import core.models.extract as extract
import core.models.load as load
import core.models.transform as transform
from common.type_def import ETL_PHASE_CALLABLE

# Third-party imports


class PipelineType(Enum):
    """Enum for valid pipeline types."""

    ETL = "ETL"
    ELT = "ELT"
    ETLT = "ETLT"


class PipelineTypes:
    """A config class that contains constants and utilities related to pipelines."""

    ALLOWED_PIPELINE_TYPES = (PipelineType.ETL.name, PipelineType.ELT.name)

    @classmethod
    def is_valid_pipeline_type(cls, pipeline_type: str) -> bool:
        """Check if a pipeline type is valid."""
        return pipeline_type in (ptype for ptype in cls.ALLOWED_PIPELINE_TYPES)


class ETLConfig:
    """A config class that contains constants related to the ETL."""

    # Pipeline Phases
    EXTRACT_PHASE = "extract"
    LOAD_PHASE = "load"
    TRANSFORM_PHASE = "transform"

    # Key used for accessing ETL steps in configurations
    STEPS_KEY = "steps"

    @staticmethod
    def get_etl_classes() -> set[ETL_PHASE_CALLABLE]:
        """Get a set of ETL interface classes."""
        return (extract.IExtractor, transform.ITransformer, load.ILoader)

    @classmethod
    def get_etl_phases(cls) -> tuple[str]:
        """Get the list of ETL phases."""
        return (cls.EXTRACT_PHASE, cls.TRANSFORM_PHASE, cls.LOAD_PHASE)

    @classmethod
    def get_base_class(cls, stage_name: str) -> ETL_PHASE_CALLABLE:
        """Returns a base ETL class associated with its respective name.

        Args:
            stage_name (str): Either extract, transform, or load.

        Returns:
            ETL_PHASE_CALLABLE: A callable class for that stage.

        Raises:
            ValueError: If an invalid stage name is provided.
        """
        # Mapping stage names to their respective interfaces
        stage_to_interface: dict[str, ETL_PHASE_CALLABLE] = {
            cls.EXTRACT_PHASE: extract.IExtractor,
            cls.LOAD_PHASE: load.ILoader,
            cls.TRANSFORM_PHASE: transform.ITransformer,
        }

        if stage_name.lower() not in stage_to_interface:
            raise ValueError(
                f"Invalid stage name: `{stage_name}`. Must be one of {list(stage_to_interface.keys())}."
            )

        return stage_to_interface.get(stage_name.lower())
