# Standard Imports
from __future__ import annotations

import asyncio
import logging
from abc import ABCMeta, abstractmethod
from functools import reduce
from typing import TYPE_CHECKING, Any

# Third Party Imports
# Project Imports
from common.decorator import async_time_it, sync_time_it

if TYPE_CHECKING:
    from common.type_def import ETLData, ExtractedData, TransformedData
    from core.plugins import PluginWrapper


if TYPE_CHECKING:
    from core.models.phases import (
        ExtractPhase,
        LoadPhase,
        TransformLoadPhase,
        TransformPhase,
    )

from core.models.exceptions import (
    ExtractError,
    LoadError,
    TransformError,
    TransformLoadError,
)
from core.models.pipeline import Pipeline, PipelineType


def plugin_sync_executor(plugin: PluginWrapper, *pipeline_args: Any, **pipeline_kwargs: Any) -> ETLData:  # noqa: ANN401
    logging.debug("Executing plugin `%s`", plugin.id)
    result = plugin.func(*pipeline_args, **pipeline_kwargs)
    logging.debug("Finished executing plugin `%s`", plugin.id)
    return result


async def plugin_async_executor(plugin: PluginWrapper, *pipeline_args: Any, **pipeline_kwargs: Any) -> ETLData:  # noqa: ANN401
    logging.debug("Executing plugin `%s`", plugin.id)
    result = await plugin.func(*pipeline_args, **pipeline_kwargs)
    logging.debug("Finished executing plugin `%s`", plugin.id)
    return result


async def task_group_executor(
    plugins: list[PluginWrapper],
    *pipeline_args: Any,  # noqa: ANN401
    **pipeline_kwargs: Any,  # noqa: ANN401
) -> dict[str, ETLData]:
    async with asyncio.TaskGroup() as group:
        tasks = {
            plugin.id: group.create_task(plugin_async_executor(plugin, *pipeline_args, **pipeline_kwargs))
            for plugin in plugins
        }

    return {plugin_id: task.result() for plugin_id, task in tasks.items()}


@async_time_it
async def run_extractor(extracts: ExtractPhase) -> ExtractedData:
    results = {}

    try:
        if extracts.pre:
            await task_group_executor(extracts.pre)

        results = await task_group_executor(extracts.steps)

        # Return the single result directly.
        if len(extracts.steps) == 1:
            return results[extracts.steps[0].id]

        # Merge data if multiple steps exist.
        return plugin_sync_executor(extracts.merge, extracted_data=results)  # type: ignore[reportArgumentType] - merge will always be populated when more than 2 extracts are provided

    except Exception as e:
        error_message = "Extraction Phase Error"
        raise ExtractError(error_message, e) from e


@sync_time_it
def run_transformer(data: ExtractedData, transformations: TransformPhase) -> TransformedData:
    if not transformations.steps:
        logging.info("No transformations to run")
        return data

    try:
        transformed_data = reduce(lambda data, plugin: plugin_sync_executor(plugin, data), transformations.steps, data)
    except Exception as e:
        msg = "Transformation Phase Error"
        raise TransformError(msg, e) from e

    return transformed_data


@async_time_it
async def run_loader(data: ExtractedData | TransformedData, destinations: LoadPhase) -> None:
    if destinations.pre:
        await task_group_executor(destinations.pre)

    try:
        await task_group_executor(destinations.steps, data=data)

        if destinations.post:
            await task_group_executor(destinations.post)
    except Exception as e:
        error_message = "Load Phase Error"
        raise LoadError(error_message, e) from e


@sync_time_it
def run_transformer_after_load(transformations: TransformLoadPhase) -> None:
    try:
        for plugin in transformations.steps:
            plugin_sync_executor(plugin)

    except Exception as e:
        error_message = "Transform Load Phase Error"
        raise TransformLoadError(error_message, e) from e


class PipelineStrategy(metaclass=ABCMeta):
    @abstractmethod
    async def execute(self, pipeline: Pipeline) -> bool:
        raise NotImplementedError("This has to be implemented by the subclasses.")


class ETLStrategy(PipelineStrategy):
    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await run_extractor(pipeline.extract)

        # Transform (CPU-bound work, so offload to executor)
        transformed_data = await asyncio.get_running_loop().run_in_executor(
            None, run_transformer, extracted_data, pipeline.transform
        )

        await run_loader(transformed_data, pipeline.load)

        return True


class ELTStrategy(PipelineStrategy):
    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await run_extractor(pipeline.extract)

        await run_loader(extracted_data, pipeline.load)

        run_transformer_after_load(pipeline.load_transform)

        return True


class ETLTStrategy(PipelineStrategy):
    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await run_extractor(pipeline.extract)

        transformed_data = await asyncio.get_running_loop().run_in_executor(
            None, run_transformer, extracted_data, pipeline.transform
        )

        await run_loader(transformed_data, pipeline.load)

        run_transformer_after_load(pipeline.load_transform)

        return True


PIPELINE_STRATEGY_MAP = {
    PipelineType.ETL: ETLStrategy,
    PipelineType.ELT: ELTStrategy,
    PipelineType.ETLT: ETLTStrategy,
}
