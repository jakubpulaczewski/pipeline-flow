""" A Pandas plug-in for extracting and loading data from S3. """

import pydantic as pyd

from common.utils.logger import setup_logger
from core.models.phases import PipelinePhase
from plugins.registry import PluginFactory

logger = setup_logger(__name__)


class PandasS3LoadPlugin(pyd.BaseModel):
    name: str

    def load(self, data):
        return "Pandas S3 Loaded Data"


def initialize():
    PluginFactory.register(PipelinePhase.EXTRACT_PHASE, "s3", PandasS3LoadPlugin)

    logger.info("S3 Plugin Succesfully Initialised")
