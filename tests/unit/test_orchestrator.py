# Standard Imports
import asyncio
from unittest.mock import AsyncMock, MagicMock

# Third Party Imports
import pytest
from pytest_cases import case, parametrize_with_cases

# Project Imports
from core.orchestrator import PipelineOrchestrator
from core.models import Pipeline


@pytest.fixture()
def etl_basic_config() -> dict[str, list]:
    """An ETL basic configuration needed to create a job."""
    return {"extract": [], "transform": [], "load": []}


@pytest.fixture
def mock_jobs(etl_basic_config: dict[str, list]) -> list[Pipeline]:
    "A mock to create a list of three jobs."
    job1 = Pipeline(name="job1", type="elt", **etl_basic_config)
    job2 = Pipeline(name="job2", type="elt", **etl_basic_config, needs=["job1"])
    job3 = Pipeline(name="job2", type="elt", **etl_basic_config, needs=["job1", "job2"])

    return [job1, job2, job3]


@case(tags="can_execute_success")
def case_no_dependency_success(
    etl_basic_config: dict[str, list]
) -> tuple[Pipeline, set[str]]:
    """A test case where there is no job dependency."""
    executed_jobs = set()
    job1 = Pipeline(name="job1", type="elt", **etl_basic_config)

    return job1, executed_jobs


@case(tags="can_execute_success")
def case_one_job_dependency_success(
    etl_basic_config: dict[str, list]
) -> tuple[Pipeline, set[str]]:
    """A test case where a job has one dependency."""
    executed_jobs = {"job1"}
    job2 = Pipeline(name="job2", type="elt", **etl_basic_config, needs="job1")

    return job2, executed_jobs


@case(tags="can_execute_success")
def case_multiple_job_dependency_success(
    etl_basic_config: dict[str, list]
) -> tuple[Pipeline, set[str]]:
    """A test case where a job has multple dependencies."""

    executed_jobs = {"job1", "job2"}
    job3 = Pipeline(name="job3", type="elt", **etl_basic_config, needs=["job1", "job2"])

    return job3, executed_jobs


@parametrize_with_cases("job, executed_jobs", cases=".", has_tag="can_execute_success")
def test_can_execute_success(job: Pipeline, executed_jobs: set) -> None:
    """A test that runs positive scenarios."""
    result = PipelineOrchestrator._can_execute(  # pylint: disable=protected-access
        job, executed_jobs
    )
    assert result is True


@case(tags="can_execute_failure")
def case_one_job_dependency_failure(
    etl_basic_config: dict[str, list]
) -> tuple[Pipeline, set[str]]:
    """A test case where a job has a dependency but the dependency is not finished executing."""
    executed_jobs = {}
    job2 = Pipeline(name="job2", type="elt", **etl_basic_config, needs="job1")

    return job2, executed_jobs


@case(tags="can_execute_failure")
def case_multiple_job_dependency_failure(
    etl_basic_config: dict[str, list]
) -> tuple[Pipeline, set[str]]:
    """A test case where a job has two dependencies but one dependency is not finished executing."""
    executed_jobs = {"job1"}
    job3 = Pipeline(name="job2", type="elt", **etl_basic_config, needs=["job1", "job2"])

    return job3, executed_jobs


@parametrize_with_cases("job, executed_jobs", cases=".", has_tag="can_execute_failure")
def test_can_execute_failures(job: Pipeline, executed_jobs: set) -> None:
    """A test that runs negative scenarios."""
    result = PipelineOrchestrator._can_execute(  # pylint: disable=protected-access
        job, executed_jobs
    )
    assert result is False


@pytest.mark.asyncio
async def test_execution_etl_pipeline(mocker, etl_basic_config: dict[str, list]) -> None:
    elt_pipeline = Pipeline(name="job1", type="elt", **etl_basic_config)

    execute_mock: MagicMock = mocker.patch.object(PipelineOrchestrator, '_execute_elt')

    # Check to verify that the pipeline has not been executed beforehand.
    assert elt_pipeline.is_executed == False

    orchestrator = await PipelineOrchestrator(pipelines=[elt_pipeline])._execute_pipeline(elt_pipeline)

    # Check to verify the mock has been executed
    assert elt_pipeline.is_executed == True
    execute_mock.assert_called_once()



async def mock_execute_pipeline(pipeline):
    await asyncio.sleep(2)
    pipeline.is_executed = True  # Modify the attribute after the sleep





@pytest.mark.asyncio
async def test_execute_pipeline(mocker, etl_basic_config):    
    execute_pipeline_mock = mocker.patch.object(
        PipelineOrchestrator, 
        "_execute_pipeline",
        new_callable=AsyncMock,
        side_effect=mock_execute_pipeline
    )


    job1 = Pipeline(name="job1", type="elt", **etl_basic_config)
    job2 = Pipeline(name="job2", type="elt", **etl_basic_config)


    orchestrator = PipelineOrchestrator(pipelines=[job1, job2])

    executed = await orchestrator.execute_pipelines()

    print(executed)






# # @pytest.mark.asyncio
# # async def test_execute_jobs_success(mocker, mock_jobs):
# #     with patch('core.models.Job.execute', new_callable=AsyncMock):
# #         for job in mock_jobs:
# #             job.execute.side_effect = lambda: asyncio.sleep(0.5)

# #     await asyncio.gather(mock_jobs[1].execute())

# #     # await executor.execute_pipelines(mock_jobs)
# #     # assert all(job._is_executed for job in mock_jobs)


# # # Create Jobs
# # job1 = Job("job1")
# # job2 = Job("job2", needs=["job1", "job3"])
# # job3 = Job("job3", needs="job4")
# # job4 = Job("job4", needs="job1")

# # # 1 -> 4 -> 3 -> 2
# # # Execute
# # asyncio.run(execute_pipelines([job1, job2, job3, job4]))
