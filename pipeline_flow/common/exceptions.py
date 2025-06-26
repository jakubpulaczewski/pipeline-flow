# Standard Imports
from __future__ import annotations

import functools
from typing import Any, Self

# Third Party Imports

# Project Imports


def docstring_message(cls: Any) -> Any:  # noqa: ANN401
    """Decorates an exception to make its docstring its default message.

    - https://stackoverflow.com/a/66491013/13618168
    """
    cls_init = cls.__init__

    @functools.wraps(cls.__init__)
    def wrapped_init(
        self: Self,
        msg: str | dict | None = None,
        *args: tuple,
        **kwargs: dict[str, Any],
    ) -> None:
        err_message: str = self.__doc__ if not msg else f"{self.__doc__}\nDetails: {msg}"
        cls_init(self, err_message, *args, **kwargs)

    cls.__init__ = wrapped_init
    return cls


@docstring_message
class ExtractError(Exception): ...


@docstring_message
class TransformError(Exception): ...


@docstring_message
class LoadError(Exception): ...


@docstring_message
class TransformLoadError(Exception): ...
