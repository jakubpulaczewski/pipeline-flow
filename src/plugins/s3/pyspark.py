""" A Pyspark plug-in for extracting and loading data from S3. """

import pydantic as pyd
from core.factory import Factory
from common.config import ETLStages
from utils.logger import setup_logger

logger = setup_logger(__name__)


class PysparkS3ExtractPlugin(pyd.BaseModel):
    source: str 
    credentials: dict

    def extract(self):
        return 'Pyspark S3 Extracted Data'


class PysparkS3LoadPlugin(pyd.BaseModel):
    source: str 
    credentials: dict
    
    def load(self, data):
        return 'Pyspark S3 Loaded data'

def initialize():
    Factory.register(ETLStages.extract.name, 's3', PysparkS3ExtractPlugin)
    Factory.register(ETLStages.load.name, 's3', PysparkS3LoadPlugin)

    print(Factory._data)
    logger.info("S3 Plugin Succesfully Initialised")

