# Standard Imports

# Project Imports
from pipeline_flow.plugins import IExtractPlugin, ILoadPlugin


class CustomExtractor(IExtractPlugin, plugin_name="custom_extract"):
    async def extract(self) -> str:
        return "CUSTOM EXTRACTED DATA"


class CustomLoader(ILoadPlugin, plugin_name="custom_load"):
    async def load(self, data: str) -> None:  # noqa: ARG002 - Data is a not used becaue it is a fake custom plugin
        # Stimulate loading data
        return
