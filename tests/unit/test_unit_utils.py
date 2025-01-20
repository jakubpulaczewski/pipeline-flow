# Standard Imports
from __future__ import annotations

import threading

# Third-party imports
import pytest

# # Project Imports
from common.utils import SingletonMeta


@pytest.fixture
def skeleton_class() -> type:
    class A(metaclass=SingletonMeta):
        val: str

    return A


def test_thread_safe_singleton_same_instance(skeleton_class: type) -> None:
    instances = []

    # Function to create and store the singleton instance
    def create_instance() -> None:
        instance = skeleton_class()
        instances.append(instance)

    # Create multiple threads to create singleton instances
    threads = [threading.Thread(target=create_instance) for _ in range(4)]

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    # Assert that all instances are the same
    assert all(instance is instances[0] for instance in instances), "Instances are not the same!"
