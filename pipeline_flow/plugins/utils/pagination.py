# Standard Imports
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any

# Third Party Imports

# Local Imports


JSON_DATA = dict[str, Any]


class PaginationStrategy(ABC):
    """A base class for pagination strategies."""

    def get_next_page(self, response: dict) -> dict:
        """A template method to extract the next page URL from the response."""
        return self.parse_next_page_from_response(response)

    @abstractmethod
    def parse_next_page_from_response(self, response: dict) -> str | None:
        """Extracts the next page URL from the response."""
        raise NotImplementedError("Pagination Strategy subclasses must implement this method.")


class PageBasedPagination(PaginationStrategy):
    def parse_next_page_from_response(self, response: dict) -> str | None:
        pagination = response.get("pagination")
        if not pagination:
            return None
        return pagination.get("next_page") if pagination.get("has_more") else None


class HATEOASPagination(PaginationStrategy):
    """Pagination strategy for APIs using HATEOAS-based links."""

    def parse_next_page_from_response(self, response: dict) -> str | None:
        links = response.get("_links", response.get("links", {}))
        return links.get("next", None)


class PaginationTypes(StrEnum):
    PAGE_BASED = "page_based"
    HATEOAS = "hateoas"

    @classmethod
    def _missing_(cls, value: str) -> "PaginationTypes":
        error_msg = f"Unknown pagination strategy: {value}. Supported values: {', '.join(s.value for s in cls)}"
        raise ValueError(error_msg)


def get_pagination_strategy(strategy: str) -> PaginationStrategy:
    """Returns a pagination strategy based on the given string."""
    match PaginationTypes(strategy.lower()):
        case PaginationTypes.PAGE_BASED:
            return PageBasedPagination()
        case PaginationTypes.HATEOAS:
            return HATEOASPagination()
