""" A Pandas plug-in for extracting and loading data from S3. """

import pydantic as pyd

from common.config import ETLStages
from common.utils.logger import setup_logger
from core.factory import PluginFactory

logger = setup_logger(__name__)


class PandasS3ExtractPlugin(pyd.BaseModel):
    source: str
    credentials: dict

    def extract(self):
        return "Pandas S3 Extracted Data"


class PandasS3LoadPlugin(pyd.BaseModel):
    source: str
    credentials: dict

    def load(self, data):
        return "Pandas S3 Loaded Data"


def initialize():
    PluginFactory.register(ETLStages.EXTRACT.name, "s3", PandasS3ExtractPlugin)
    PluginFactory.register(ETLStages.LOAD.name, "s3", PandasS3LoadPlugin)

    print(PluginFactory._data)
    logger.info("S3 Plugin Succesfully Initialised")
