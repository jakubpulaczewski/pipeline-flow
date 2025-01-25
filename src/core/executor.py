# Standard Imports
from __future__ import annotations

import asyncio
import logging
from abc import ABCMeta, abstractmethod
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

logger = logging.getLogger(__name__)


def plugin_sync_executor(plugin: PluginWrapper, *pipeline_args: Any, **pipeline_kwargs: Any) -> ETLData:  # noqa: ANN401
    try:
        return plugin.func(*pipeline_args, **pipeline_kwargs)
    except Exception:
        error_msg = f"Error has occured during plugin `{plugin.id}` execution"
        logging.exception(error_msg)
        raise


async def plugin_async_executor(plugin: PluginWrapper, *pipeline_args: Any, **pipeline_kwargs: Any) -> ETLData:  # noqa: ANN401
    try:
        return await plugin.func(*pipeline_args, **pipeline_kwargs)
    except Exception:
        error_msg = f"Error has occured during plugin `{plugin.id}` execution"
        logging.exception(error_msg)
        raise


async def task_executor(
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
            await task_executor(extracts.pre)

        results = await task_executor(extracts.steps)

        # Return the single result directly.
        if len(extracts.steps) == 1:
            return results[extracts.steps[0].id]

        # Merge data if multiple steps exist.
        return plugin_sync_executor(extracts.merge, extracted_data=results)  # type: ignore[reportArgumentType] - merge will always be populated when more than 2 extracts are provided

    except Exception as e:
        error_message = f"Error during `extraction`: {e}"
        logger.exception(error_message)
        raise ExtractError(error_message) from e


@sync_time_it
def run_transformer(data: ExtractedData, transformations: TransformPhase) -> TransformedData:
    try:
        for plugin in transformations.steps:
            logger.info("Applying transformation: %s", plugin.id)
            transformed_data = plugin_sync_executor(plugin, data)
            data = transformed_data  # Pass the transformed data to the next step

    except Exception as e:
        error_message = "Error during `transform`"
        logger.exception(error_message)
        raise TransformError(error_message) from e

    return transformed_data  # type: ignore[reportPossiblyUnboundVariable]


@async_time_it
async def run_loader(data: ExtractedData | TransformedData, destinations: LoadPhase) -> list:
    if destinations.pre:
        await task_executor(destinations.pre)

    try:
        results = await task_executor(destinations.steps, data=data)

        if destinations.post:
            await task_executor(destinations.post)
    except Exception as e:
        error_message = f"Error during `load`): {e}"
        logger.exception(error_message)
        raise LoadError(error_message) from e

    return [{"id": plugin_id, "success": True} for plugin_id, _ in results.items()]


@sync_time_it
def run_transformer_after_load(transformations: TransformLoadPhase) -> list[dict[str, bool]]:
    results = []

    try:
        for plugin in transformations.steps:
            plugin_sync_executor(plugin)
            results.append({"id": plugin.id, "success": True})

    except Exception as e:
        error_message = "Error during `transform_at_load`"
        logger.exception("Error during `transform_at_load`")
        raise TransformLoadError(error_message) from e
    return results


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
