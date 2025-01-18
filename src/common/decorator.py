# Standard Imports
import asyncio
import logging
import time
from functools import wraps
from typing import Callable, TypeVar, ParamSpec, Awaitable

# Third-party Imports

# Project Imports


logger = logging.getLogger(__name__)


R = TypeVar("R")
P = ParamSpec("P")



def async_time_it(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        """Wrapper for asynchronous functions."""
        start = asyncio.get_running_loop().time()
        try:
            result = await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error occurred while executing {func.__name__}: {e}")
            raise
        finally:
            total_time = asyncio.get_running_loop().time() - start
            logger.info(
                f"Time taken to execute {func.__name__} is {total_time:.4f} seconds"
            )
        return result  
    return inner

def sync_time_it(func: Callable[P, R]) -> Callable[P, R]: 
    @wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        """Wrapper for synchronous functions."""
        start = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error occurred while executing {func.__name__}: {e}")
            raise
        finally:
            total_time = time.time() - start
            logger.info(
                f"Time taken to execute {func.__name__} is {total_time:.4f} seconds"
            )
        return result
    return inner