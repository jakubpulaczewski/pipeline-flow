# Standard Imports
import asyncio
from unittest.mock import AsyncMock

# Third Party Imports
import pytest

# Project Imports
from core.models.phase_wrappers import ExtractResult
from core.models.exceptions import ExtractException
from core.orchestrator import PipelineOrchestrator
from core.pipeline_strategy import PipelineType, PipelineStrategyFactory, ETLStrategy


def test_can_execute_no_depedency(etl_pipeline_factory) -> None:
    job1 = etl_pipeline_factory(name="Job1")
    executed_jobs = set()

    result = PipelineOrchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is True


def test_can_execute_one_depedency(etl_pipeline_factory) -> None:
    job1 = etl_pipeline_factory(name="Job2", needs="Job1")
    executed_jobs = {"Job1"}

    result = PipelineOrchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is True


def test_can_execute_multiple_depedencies(etl_pipeline_factory) -> None:
    job1 = etl_pipeline_factory(name="Job3", needs=["Job1", "Job2"])
    executed_jobs = {"Job1", "Job2"}

    result = PipelineOrchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is True


def test_can_execute_one_job_dependency_failure(etl_pipeline_factory) -> None:
    job1 = etl_pipeline_factory(name="Job1", needs=["Job2"])
    executed_jobs = {}

    result = PipelineOrchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is False


def test_can_execute_multiple_job_dependencies_failure(etl_pipeline_factory) -> None:
    job1 = etl_pipeline_factory(name="Job3", needs=["Job1", "Job2"])
    executed_jobs = {"Job1"}

    result = PipelineOrchestrator._can_execute(  # pylint: disable=protected-access
        job1, executed_jobs
    )
    assert result is False


@pytest.mark.asyncio
async def test_pipeline_queue_producer(etl_pipeline_factory) -> None:
    jobs = [
        etl_pipeline_factory(name="Job1"),
        etl_pipeline_factory(name="Job2", needs=["Job1"])
    ]
    orchestrator = PipelineOrchestrator()
    await orchestrator.pipeline_queue_producer(jobs)

    assert orchestrator.pipeline_queue.qsize() == 2

    await orchestrator.pipeline_queue.get()

    assert orchestrator.pipeline_queue.qsize() == 1


@pytest.mark.asyncio
async def test_empty_pipeline_queue_exception() -> None:
    orchestrator = PipelineOrchestrator()

    with pytest.raises(ValueError, match="The Pipeline queue is empty. There is nothing to execute."):
        await orchestrator._execute_pipeline()


@pytest.mark.asyncio
async def test_execute_pipeline_exception(mocker, etl_pipeline_factory) -> None:
    # Setup
    mocker.patch.object(PipelineStrategyFactory, "get_pipeline_strategy", return_value=ETLStrategy)

    execute_mock = mocker.patch.object(ETLStrategy, "execute", 
        side_effect=ExtractException("Error during extraction: error123", ExtractResult(name='extractor_id', success=False, error='error123')))

    etl_pipeline = etl_pipeline_factory(name="Job1")
    orchestrator = PipelineOrchestrator()

    await orchestrator.pipeline_queue_producer([etl_pipeline])

    # When
    with pytest.raises(ExtractException):
        await orchestrator._execute_pipeline()

    # Then
    assert orchestrator.pipeline_queue.qsize() == 0
    execute_mock.assert_awaited_once_with(etl_pipeline)



@pytest.mark.asyncio
async def test_etl_pipeline_execution(mocker, etl_pipeline_factory) -> None:
    # Setup
    pipeline_strategy_mock = mocker.patch.object(PipelineStrategyFactory, "get_pipeline_strategy", return_value=ETLStrategy)
    execute_mock = mocker.patch.object(ETLStrategy, "execute", return_value=True)

    etl_pipeline = etl_pipeline_factory(name="Job1")
    orchestrator = PipelineOrchestrator()

    await orchestrator.pipeline_queue_producer([etl_pipeline])
    
    # When
    await orchestrator._execute_pipeline()

    # THen
    assert orchestrator.pipeline_queue.qsize() == 0
    assert execute_mock.await_count == 1
    assert etl_pipeline.is_executed is True

    execute_mock.assert_awaited_once_with(etl_pipeline)
    pipeline_strategy_mock.assert_called_once_with(PipelineType('ETL'))



async def execute_pipeline_mock(jobs):
    """Custom side effect factory for mocking _execute_pipeline."""
    async def side_effect():
        current_job = jobs.pop(0)  # Get the first job
        await asyncio.sleep(1)  # Simulate asynchronous work
        current_job.is_executed = True  # Modify the job
        return True
    return side_effect


@pytest.mark.asyncio
async def test_execute_pipelines_success(mocker, etl_pipeline_factory) -> None:
    job1 = etl_pipeline_factory(name="Job1")
    job2 = etl_pipeline_factory(name="Job2")
    job3 = etl_pipeline_factory(name="Job3", needs=["Job1", "Job2"])
    jobs = [job1, job2, job3]

    executor_pipeline_mock = mocker.patch.object(
        PipelineOrchestrator,
        "_execute_pipeline",
        new_callable=AsyncMock,
        side_effect=await execute_pipeline_mock(jobs),
    )
    orchestrator = PipelineOrchestrator()

    executed = await orchestrator.execute_pipelines(pipelines=jobs)

    assert executed == {'Job1', 'Job2', "Job3"}
    assert job1.is_executed == True
    assert job2.is_executed == True
    assert job3.is_executed == True

    assert len(executor_pipeline_mock.mock_calls) == 3




@pytest.mark.asyncio
async def test_execute_pipelines_circular_dependency(etl_pipeline_factory) -> None:
    job1 = etl_pipeline_factory(name="Job1", needs="Job2")
    job2 = etl_pipeline_factory(name="Job2", needs="Job1")
    jobs = [job1, job2]

    with pytest.raises(ValueError, match="Circular dependency detected!"):
        await PipelineOrchestrator().execute_pipelines(pipelines=jobs)


@pytest.mark.asyncio
async def test_execute_pipelines_no_pipelines() -> None:
    orchestrator = PipelineOrchestrator()

    with pytest.raises(ValueError, match="The Pipeline list is empty. There is nothing to execute."):
        executed = await orchestrator.execute_pipelines(pipelines=[])

