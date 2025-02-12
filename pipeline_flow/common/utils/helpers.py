# Standard Imports
from __future__ import annotations

import asyncio
import logging
import threading
import time
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar

# Project Imports

# Third-party imports


# Type Imports
if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


def async_time_it[**P, R](func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        """Wrapper for asynchronous functions."""
        start = asyncio.get_running_loop().time()
        try:
            result = await func(*args, **kwargs)
        except Exception:
            logging.error("Error occurred while executing %s: %s")
            raise
        finally:
            total_time = asyncio.get_running_loop().time() - start
            msg = f"Time taken to execute {func.__name__} is {total_time:.4f} seconds"
            logging.info(msg)
        return result

    return inner


def sync_time_it[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    @wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        """Wrapper for synchronous functions."""
        start = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception:
            logging.error("Error occurred while executing %s: %s")
            raise
        finally:
            total_time = time.time() - start
            msg = f"Time taken to execute {func.__name__} is {total_time:.4f} seconds"
            logging.info(msg)
        return result

    return inner


class SingletonMeta[T](type):
    _instances: ClassVar[dict] = {}

    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> T:  #  noqa: ANN401
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]
