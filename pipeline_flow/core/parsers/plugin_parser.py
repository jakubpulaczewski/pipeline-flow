from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from pipeline_flow.common.type_def import PluginRegistryJSON


class PluginParser:
    def __init__(self: Self, plugins_payload: PluginRegistryJSON) -> None:
        self.plugins_payload = plugins_payload

    @staticmethod
    def get_all_files(paths: list[str]) -> set[str]:
        files = set()
        for path in paths:
            if os.path.isdir(path):  # noqa: PTH112
                for filename in os.listdir(path):
                    if filename.endswith(".py"):
                        full_path = Path(path) / filename
                        files.add(str(full_path))
            elif path.endswith(".py"):
                files.add(path)

        return files

    def fetch_custom_plugin_files(self: Self) -> set[str]:
        custom_payload = self.plugins_payload.get("custom", {})
        if not custom_payload:
            logging.debug("No custom plugins found in the YAML.")
            return set()

        # Gather files from dirs and individual files
        files_from_dir = self.get_all_files(custom_payload.get("dirs", set()))
        files = self.get_all_files(custom_payload.get("files", set()))

        # Combine both sets of files
        return files_from_dir.union(files)

    def fetch_community_plugin_modules(self: Self) -> set[str]:
        comm_payload = self.plugins_payload.get("community", {})
        if not comm_payload:
            logging.debug("No community plugins found in the YAML.")
            return set()

        base_module = "community.plugins."

        return {base_module + plugin for plugin in comm_payload}
