import asyncio

DEFAULT_PLUGINS = ["s3"]


def _can_execute(job, executed_jobs) -> bool:
    """A function that checks if a given job is executable or has an external dependency."""
    if job.needs is None:
        return True
    if isinstance(job.needs, str):
        return job.needs in executed_jobs
    if isinstance(job.needs, list):
        return all(need in executed_jobs for need in job.needs)
    return False


async def execute_jobs(jobs) -> set:
    """Asynchronously executes parsed jobs."""
    executed_jobs = set()

    while jobs:
        executable_jobs = [job for job in jobs if _can_execute(job, executed_jobs)]

        if not executable_jobs:
            raise ValueError("Circular dependency detected!")

        await asyncio.gather(*(job.execute() for job in executable_jobs))

        executed_jobs.update(job.name for job in executable_jobs)
        jobs = [job for job in jobs if not job.is_executed]

    return executed_jobs
