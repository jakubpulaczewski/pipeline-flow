from enum import Enum
from typing import Type

from core.models import ExtractInterface, LoadInterface, TransformInterface


class ETLStages(Enum):
    """An enum class to group ETL stages under a single namespace."""

    EXTRACT = ExtractInterface
    TRANSFORM = TransformInterface
    LOAD = LoadInterface

    @classmethod
    def get_all_stages(cls) -> list[str]:
        """Returns a list of all ETL Stages."""
        return [stage.name for stage in cls]

    @classmethod
    def get_all_classes(
        cls,
    ) -> list[Type[ExtractInterface | LoadInterface | TransformInterface]]:
        """Returns a list of all ETL Base Interfaces classes."""

        return [stage.value for stage in cls]


ETL_CLASS = ETLStages.get_all_classes()
ETL_STAGES = ETLStages.get_all_stages()
