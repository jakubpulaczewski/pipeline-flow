# Standard Imports
import asyncio
from collections.abc import Callable

# Third Party Imports
import pytest
from pytest_mock import MockerFixture

from core.executor import ETLStrategy

# Project Imports
from core.models.exceptions import ExtractError
from core.models.pipeline import Pipeline
from core.orchestrator import PipelineOrchestrator
from core.parser import YamlConfig


@pytest.fixture
def orchestrator() -> PipelineOrchestrator:
    config = YamlConfig(engine="native", concurrency=2)
    return PipelineOrchestrator(config=config)


def test_can_execute_no_depedency(
    orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    job1 = etl_pipeline_factory(name="Job1")
    executed_jobs = set()

    result = orchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is True


def test_can_execute_one_depedency(
    orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    job1 = etl_pipeline_factory(name="Job2", needs="Job1")
    executed_jobs = {"Job1"}

    result = orchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is True


def test_can_execute_multiple_depedencies(
    orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    job1 = etl_pipeline_factory(name="Job3", needs=["Job1", "Job2"])
    executed_jobs = {"Job1", "Job2"}

    result = orchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is True


def test_can_execute_one_job_dependency_failure(
    orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    job1 = etl_pipeline_factory(name="Job1", needs=["Job2"])
    executed_jobs = {}

    result = orchestrator._can_execute(  # pylint: disable=protected-access
        job1,
        executed_jobs,  # type: ignore[reportArgumentType]
    )
    assert result is False


def test_can_execute_multiple_job_dependencies_failure(
    orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    job1 = etl_pipeline_factory(name="Job3", needs=["Job1", "Job2"])
    executed_jobs = {"Job1"}

    result = orchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is False


@pytest.mark.asyncio
async def test_pipeline_queue_producer(
    orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    jobs = [
        etl_pipeline_factory(name="Job1"),
        etl_pipeline_factory(name="Job2", needs=["Job1"]),
    ]
    await orchestrator.pipeline_queue_producer(jobs)

    assert orchestrator.pipeline_queue.qsize() == 2

    await orchestrator.pipeline_queue.get()

    assert orchestrator.pipeline_queue.qsize() == 1


@pytest.mark.asyncio
async def test_execute_pipeline_exception(
    mocker: MockerFixture, orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    # Setup
    execute_mock = mocker.patch.object(
        ETLStrategy,
        "execute",
        side_effect=ExtractError("Error during extraction: error123"),
    )

    etl_pipeline = etl_pipeline_factory(name="Job1")

    await orchestrator.pipeline_queue_producer([etl_pipeline])

    # When
    with pytest.raises(ExtractError):
        await orchestrator._execute_pipeline()

    # Then
    assert orchestrator.pipeline_queue.qsize() == 0
    execute_mock.assert_awaited_once_with(etl_pipeline)


@pytest.mark.asyncio
async def test_etl_pipeline_execution(
    mocker: MockerFixture, orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    # Setup
    execute_mock = mocker.patch.object(ETLStrategy, "execute", return_value=True)

    etl_pipeline = etl_pipeline_factory(name="Job1")

    await orchestrator.pipeline_queue_producer([etl_pipeline])

    # When
    await orchestrator._execute_pipeline()

    # THen
    assert orchestrator.pipeline_queue.qsize() == 0
    assert execute_mock.await_count == 1
    assert etl_pipeline.is_executed is True

    execute_mock.assert_awaited_once_with(etl_pipeline)


@pytest.mark.asyncio
async def test_execute_pipelines_success(
    mocker: MockerFixture, orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    job1 = etl_pipeline_factory(name="Job1")
    job2 = etl_pipeline_factory(name="Job2")
    job3 = etl_pipeline_factory(name="Job3", needs=["Job1", "Job2"])
    jobs = [job1, job2, job3]

    async def execute_pipeline_mock() -> None:
        if jobs:
            current_job = jobs.pop(0)  # Get the first job
            await asyncio.sleep(0.1)  # Simulate asynchronous work
            current_job.is_executed = True  # Modify the job
            return

    mocker.patch.object(PipelineOrchestrator, "_execute_pipeline", side_effect=execute_pipeline_mock)

    executed = await orchestrator.execute_pipelines(pipelines=jobs)

    assert executed == {"Job1", "Job2", "Job3"}
    assert job1.is_executed is True
    assert job2.is_executed is True
    assert job3.is_executed is True


@pytest.mark.asyncio
async def test_execute_pipelines_circular_dependency(
    orchestrator: PipelineOrchestrator, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    job1 = etl_pipeline_factory(name="Job1", needs="Job2")
    job2 = etl_pipeline_factory(name="Job2", needs="Job1")
    jobs = [job1, job2]

    with pytest.raises(ValueError, match="Circular dependency detected!"):
        await orchestrator.execute_pipelines(pipelines=jobs)


@pytest.mark.asyncio
async def test_execute_pipelines_no_pipelines(orchestrator: PipelineOrchestrator) -> None:
    with pytest.raises(ValueError, match="The Pipeline list is empty. There is nothing to execute."):
        await orchestrator.execute_pipelines(pipelines=[])


@pytest.mark.asyncio
async def test_execute_pipelines_semaphore_lock(
    mocker: MockerFixture, etl_pipeline_factory: Callable[..., Pipeline]
) -> None:
    # Setup
    jobs = [etl_pipeline_factory(name="Job" + str(index)) for index in range(10)]

    concurrency = 2
    execution_time = 0.2

    config = YamlConfig(concurrency=concurrency)
    orchestrator = PipelineOrchestrator(config)

    async def execute_mock(pipeline: Pipeline) -> bool:  # noqa: ARG001
        await asyncio.sleep(execution_time)
        return True

    start = asyncio.get_running_loop().time()
    with mocker.patch.object(ETLStrategy, "execute", side_effect=execute_mock):
        await orchestrator.execute_pipelines(pipelines=jobs)

    total_execution_time = asyncio.get_running_loop().time() - start
    expected_execution_time = (len(jobs) / concurrency) * execution_time

    assert expected_execution_time + 0.1 > total_execution_time >= expected_execution_time
