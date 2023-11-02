""" A Pandas plug-in for extracting and loading data from S3. """

import pydantic as pyd
from core.factory import Factory
from common.config import ETLStages
from utils.logger import setup_logger

logger = setup_logger(__name__)


class PandasS3ExtractPlugin(pyd.BaseModel):
    source: str 
    credentials: dict

    def extract(self):
        return 'Pandas S3 Extracted Data'


class PandasS3LoadPlugin(pyd.BaseModel):
    source: str 
    credentials: dict
    
    def load(self, data):
        return 'Pandas S3 Loaded Data'

def initialize():
    Factory.register(ETLStages.extract.name, 's3', PandasS3ExtractPlugin)
    Factory.register(ETLStages.load.name, 's3', PandasS3LoadPlugin)

    print(Factory._data)
    logger.info("S3 Plugin Succesfully Initialised")

