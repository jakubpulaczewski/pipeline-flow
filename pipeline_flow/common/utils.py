# Standard Imports
import threading
from typing import Any, ClassVar

# Project Imports

# Third-party imports


class SingletonMeta[T](type):
    _instances: ClassVar[dict] = {}

    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> T:  #  noqa: ANN401
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]
