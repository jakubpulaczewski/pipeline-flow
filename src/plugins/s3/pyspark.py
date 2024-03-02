""" A Pyspark plug-in for extracting and loading data from S3. """

import pydantic as pyd

from common.config import ETLStages
from common.utils.logger import setup_logger
from core.factory import PluginFactory

logger = setup_logger(__name__)


class PysparkS3ExtractPlugin(pyd.BaseModel):
    source: str
    credentials: dict

    def extract(self):
        return "Pyspark S3 Extracted Data"


class PysparkS3LoadPlugin(pyd.BaseModel):
    source: str
    credentials: dict

    def load(self, data):
        return "Pyspark S3 Loaded data"


def initialize():
    PluginFactory.register(ETLStages.EXTRACT.name, "s3", PysparkS3ExtractPlugin)
    PluginFactory.register(ETLStages.LOAD.name, "s3", PysparkS3LoadPlugin)

    print(PluginFactory._data)
    logger.info("S3 Plugin Succesfully Initialised")
