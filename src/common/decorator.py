# Standard Imports
import asyncio
import time
import logging

from functools import wraps
from typing import Callable, TypeVar, Any

# Third-party Imports

# Project Imports


logger = logging.getLogger(__name__)

T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])

def time_it(func: F) -> F:
    @wraps(func)
    def sync_inner(*args: Any, **kwargs: Any) -> T:
        """Inner wrapper function for synchronouos functions."""
        start = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error occurred while executing {func.__name__}: {e}")
            raise
        finally:
             total_time = time.time() - start
             logger.info(f"Time taken to execute {func.__name__} is {total_time:.4f} seconds")
        return result
    
    @wraps(func)
    async def async_inner(*args: Any, **kwargs: Any) -> T:
        """Inner wrapper function for asynchronous functions."""
        start = asyncio.get_running_loop().time()
        try:
            result = await func(*args, **kwargs)
        except Exception as e:
           logger.error(f"Error occurred while executing {func.__name__}: {e}")
           raise
        finally:
             total_time = asyncio.get_running_loop().time() - start
             logger.info(f"Time taken to execute {func.__name__} is {total_time:.4f} seconds")
        return result
        
    if asyncio.iscoroutinefunction(func):
        return async_inner
    else:
        return sync_inner 