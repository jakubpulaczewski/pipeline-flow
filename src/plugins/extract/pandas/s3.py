""" A Pandas plug-in for extracting and loading data from S3. """

import pydantic as pyd

from common.utils.logger import setup_logger
from core.models.phases import PipelinePhase
from core.plugins import PluginFactory

logger = setup_logger(__name__)


class PandasS3ExtractPlugin(pyd.BaseModel):
    name: str

    def extract(self):
        return "Pandas S3 Extracted Data"


def initialize():
    PluginFactory.register(PipelinePhase.EXTRACT_PHASE, "s3", PandasS3ExtractPlugin)

    logger.info("S3 Plugin Succesfully Initialised")
