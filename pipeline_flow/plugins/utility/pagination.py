# Standard Imports
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Self

# Third Party Imports
# Local Imports
from pipeline_flow.plugins import IPlugin


class IPaginationHandler(ABC, IPlugin, interface=True):
    """A base class for handling pagination in extract plugins."""

    @abstractmethod
    def __call__(self: Self, response: dict) -> str | None:
        """Asynchronously fetch data from the API endpoint and handle pagination."""
        raise NotImplementedError("Subclasses must implement this method.")


class PageBasedPagination(IPaginationHandler, plugin_name="page_based_pagination"):
    def __call__(self: Self, response: dict) -> str | None:
        pagination = response.get("pagination")
        if not pagination:
            return None
        return pagination.get("next_page") if pagination.get("has_more") else None


class HATEOASPagination(IPaginationHandler, plugin_name="hateoas_pagination"):
    """Pagination strategy for APIs using HATEOAS-based links."""

    def __call__(self: Self, response: dict) -> str | None:
        links = response.get("_links", response.get("links", {}))
        return links.get("next", None)
