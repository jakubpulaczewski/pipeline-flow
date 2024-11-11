# # Standard Imports
# import random
# from typing import Any, Type

# # Third-party imports

# # Project Imports
# from common.config import ETLConfig
# from common.type_def import ETL_CALLABLE

# from core.models import (
#     IExtractor,
#     ILoader,
#     ITransformer
# )


# def generate_plugin_name(stage: str) -> str:
#     """ Randomly takes a random plugin name from a default list for a given ETL stage.

#     Args:
#         stage (str): An ETL Stage - either extract or load.

#     Returns:
#         str: a string plugin name
#     """
#     random_plugins = ['jdbc', 's3', 'api', 'ftp', 'http', 'https']
#     return random.choice(random_plugins)


# def create_fake_class(key: str, stage: str) -> ETL_CALLABLE:
#     """Creates a fake class using one of the ETL Interfaces.

#     Args:
#         key (str): Name of the plugin for the ETL stage.

#     Returns:
#         ETL_CALLABLE: an ETL class associated with that specific stage.
#     """
#     if isinstance(key, str):
#         stage_class = ETLConfig.get_base_class(stage)
#         return type(key, (stage_class,), {})
