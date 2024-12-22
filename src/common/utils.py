# Standard Imports
import asyncio
import threading
import functools as fn

from concurrent.futures import Executor
from typing import  Any

# Third-party imports

# Project Imports
from common.type_def import TransformedData
class SingletonMeta(type):

    _instances = {}

    _lock: threading.Lock = threading.Lock()


    def __call__(cls, *args, **kwargs):

        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


async def run_in_executor(
    executor: Executor | None,
    func: Any,
    *args: Any,
    **kwargs: Any,
) -> TransformedData :
    """
    Run asyncio's executor asynchronously.

    If the executor is None, use the default ThreadPoolExecutor.
    """
    return await asyncio.get_running_loop().run_in_executor(
        executor,
        fn.partial(func, *args, **kwargs),
    )