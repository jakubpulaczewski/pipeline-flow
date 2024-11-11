# Standard Imports
from __future__ import annotations

from typing import Type

# Project Imports
import core.models.extract as extract
import core.models.load as load
import core.models.transform as transform
from common.type_def import ETL_CALLABLE

# Third-party imports


class PipelineTypes:
    """A config class that contains constants related to Pipelines and its types."""

    ETL_PIPELINE = "ETL"
    ELT_PIPELINE = "ELT"
    ETLT_PIPELINE = "ETLT"

    ALLOWED_PIPELINE_TYPES = [ETL_PIPELINE, ELT_PIPELINE]


class ETLConfig:
    """A config class that contains constants related to the ETL."""

    # Defining stage name constants
    EXTRACT = "extract"
    LOAD = "load"
    TRANSFORM = "transform"
    STEPS = "steps"

    @staticmethod
    def get_etl_classes() -> (
        set[ETL_CALLABLE]
    ):  # TOOD: Not sure if it needs to be set or tuple.
        return {extract.IExtractor, transform.ITransformer, load.ILoader}

    @classmethod
    def get_etl_phases(cls) -> tuple[str]:
        return (cls.EXTRACT, cls.TRANSFORM, cls.LOAD)

    @classmethod
    def get_base_class(cls, stage_name: str) -> ETL_CALLABLE:
        """Returns a base ETL class associated of its respective name.

        Args:
            stage_name (str): Either extract, transform or load.

        Returns:
            ETL_CALLABLE: A callable class for that stage
        """
        # Mapping stage names to their respective interfaces
        stage_to_interface: dict[str, ETL_CALLABLE] = {
            cls.EXTRACT: extract.IExtractor,
            cls.LOAD: load.ILoader,
            cls.TRANSFORM: transform.ITransformer,
        }

        return stage_to_interface.get(stage_name.lower(), None)
