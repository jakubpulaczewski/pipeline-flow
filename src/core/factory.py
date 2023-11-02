from typing import Callable
from core.models import (
    ExtractInterface, 
    TransformInterface, 
    LoadInterface
)
from utils.logger import setup_logger

ETL_CLASS = ExtractInterface | TransformInterface | LoadInterface

logger = setup_logger(__name__)

class Factory:
    _data = {}


    @classmethod
    def register(cls, etl_stage, service, plugin) -> None:
        """Add or update a plugin for a given ETL type and service."""
        # TODO: In the future should get the instance of the protocol implemented classs and self derive it.
        if etl_stage not in cls._data:
            cls._data[etl_stage] = {}
        
        if service not in cls._data[etl_stage]:
            cls._data[etl_stage][service] = plugin
        else:
            logger.warning(f"The service for {etl_stage} stage already exists in Factory class.")


    @classmethod
    def remove(cls, etl_stage, service) -> None:
        """Remove a plugin for a given ETL type and service."""
        if etl_stage in cls._data and service in cls._data[etl_stage]:
            del cls._data[etl_stage][service]
            # Remove the ETL type dict if empty after removing the service.
            if not cls._data[etl_stage]:
                del cls._data[etl_stage]

    @classmethod
    def get(cls, etl_stage, service) -> Callable[..., ETL_CLASS]:
        """Retrieve a plugin for a given ETL type and service."""
        return cls._data.get(etl_stage, {}).get(service, None)



