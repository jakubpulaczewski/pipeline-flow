# Standard Imports
from __future__ import annotations

import asyncio
import logging
import time
from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

# Third-party Imports

# Project Imports


logger = logging.getLogger(__name__)


def async_time_it[**P, R](func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        """Wrapper for asynchronous functions."""
        start = asyncio.get_running_loop().time()
        try:
            result = await func(*args, **kwargs)
        except Exception:
            logger.exception("Error occurred while executing %s: %s")
            raise
        finally:
            total_time = asyncio.get_running_loop().time() - start
            msg = f"Time taken to execute {func.__name__} is {total_time:.4f} seconds"
            logger.info(msg)
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
            logger.exception("Error occurred while executing %s: %s")
            raise
        finally:
            total_time = time.time() - start
            msg = f"Time taken to execute {func.__name__} is {total_time:.4f} seconds"
            logger.info(msg)
        return result

    return inner
