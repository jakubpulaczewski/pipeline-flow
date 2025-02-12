# Standard Imports
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

# Project Imports
from pipeline_flow.core.registry import PluginRegistry

if TYPE_CHECKING:
    from pipeline_flow.plugins import IPlugin


def serialize_plugin(value: dict) -> IPlugin:
    return PluginRegistry.instantiate_plugin(value)


def serialize_plugins(value: list) -> list[IPlugin]:
    return [PluginRegistry.instantiate_plugin(plugin_dict) for plugin_dict in value]


def unique_id_validator(steps: list[IPlugin]) -> list[IPlugin]:
    if not steps:
        logging.debug("No plugins to validate for unique ID.")
        return steps

    ids = {}

    for step in steps:
        if step.id in ids:
            raise ValueError("The `ID` is not unique. There already exists an 'ID' with this name.")

        ids[step.id] = 1

    return steps
