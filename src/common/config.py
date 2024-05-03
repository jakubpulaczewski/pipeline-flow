# Standard Imports
from __future__ import annotations
from typing import Type

# Third-party imports

# Project Imports
import core.models as models


class ETLConfig:
    """ A config class that contains constants related to the ETL."""
    # Type alias for the union of all class types
    ETL_CALLABLE = Type[models.ExtractInterface | models.TransformInterface | models.LoadInterface]
    ETL_INSTANCE = models.ExtractInterface | models.TransformInterface | models.LoadInterface

    # Defining stage name constants
    EXTRACT = "extract"
    LOAD = "load"
    TRANSFORM = "transform"

    # Mapping stage names to their respective interfaces
    STAGE_TO_INTERFACE: dict[str, ETL_CALLABLE] = {
        EXTRACT: models.ExtractInterface,
        LOAD: models.LoadInterface,
        TRANSFORM: models.TransformInterface,
    }
    
    ETL_STAGES: set[str] = {EXTRACT, LOAD, TRANSFORM}
    ETL_CLASSES: set[ETL_CALLABLE] = {models.ExtractInterface, models.TransformInterface, models.LoadInterface}

    @classmethod
    def get_base_class(cls,stage_name: str) -> ETL_CALLABLE:
        """ Returns a base ETL class associated of its step.

        Args:
            stage_name (str): Either extract, transform or load.

        Returns:
            ETL_CALLABLE: A callable class for that stage
        """
        return cls.STAGE_TO_INTERFACE.get(stage_name.lower(), None)
    