from __future__ import annotations

import importlib.util
import logging
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipeline_flow.common.type_def import PluginRegistryJSON

from pipeline_flow.core.parsers import PluginParser


def load_plugins(engine: str, plugins_payload: PluginRegistryJSON | None) -> None:
    """Invoke all methods to load plugins."""
    logging.info("Starting to load plugins...")

    if not plugins_payload:
        logging.warning("No plugins to load.")
        return

    plugin_parser = PluginParser(plugins_payload)

    # Load core engine transformations
    load_core_engine_transformations(engine)

    # Load custom plugins
    custom_files = plugin_parser.fetch_custom_plugin_files()
    load_custom_plugins(custom_files)

    # Load community plugins
    community_modules = plugin_parser.fetch_community_plugin_modules()
    load_community_plugins(community_modules)

    logging.info("All plugins loaded successfully.")


def _load_plugin_from_file(plugin_file: str) -> None:
    # Get the module name from the file and remove .py extension
    if plugin_file.startswith("/"):
        fq_module_name = plugin_file.replace(os.sep, ".")[1:-3]
    else:
        fq_module_name = plugin_file.replace(os.sep, ".")[:-3]

    # Check if the module is already loaded to avoid re-importing
    if fq_module_name in sys.modules:
        logging.debug("Module %s has been re-loaded.", fq_module_name)
        return

    try:
        logging.debug("Loading module %s", fq_module_name)
        spec = importlib.util.spec_from_file_location(fq_module_name, plugin_file)

        if not spec:
            raise ImportError(  # noqa: TRY301
                "The Spec based on following file location is empty: %s and %s plugin.", fq_module_name, plugin_file
            )

        plugin_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(plugin_module)  # type: ignore[reportOptionalMemberAccess]
        logging.info("Loaded plugin from %s as %s", plugin_file, fq_module_name)

    except ImportError:
        msg = f"Error importing plugin from `{fq_module_name}` module,"
        logging.error(msg)
        raise


def load_core_engine_transformations(engine: str) -> None:
    core_engine_file = f"pipeline_flow/plugins/transform/{engine}.py"
    _load_plugin_from_file(core_engine_file)


def load_custom_plugins(custom_files: set[str]) -> None:
    if not custom_files:
        return
    logging.info("Planning to load all custom files: %s", custom_files)
    for custom_file in custom_files:
        _load_plugin_from_file(custom_file)

    logging.info("Loaded all custom plugins.")


def load_community_plugins(community_modules: set[str]) -> None:
    if not community_modules:
        return
    logging.info("Planning to load all community modules: %s", ",".join(list(community_modules)))
    for module in community_modules:
        _load_plugin_from_file(module)
    logging.info("Loaded all community plugins/")
