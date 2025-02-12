# Standard Imports
import asyncio
import logging
import os
from http import HTTPStatus
from typing import Any, Self

# Third Party Imports
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random

# Local Imports
from pipeline_flow.plugins import IExtractPlugin
from pipeline_flow.plugins.utils.pagination import PaginationStrategy, PaginationTypes, get_pagination_strategy

JSON_DATA = dict[str, Any]


async def async_sleep(seconds: float) -> None:
    logging.debug("Retrying in %s seconds", seconds)
    await asyncio.sleep(seconds)


class RestApiAsyncExtractor(IExtractPlugin, plugin_name="rest_api_async_extractor"):
    def __init__(
        self: Self,
        plugin_id: str,
        base_url: str,
        endpoint: str,
        pagination_type: str = PaginationTypes.PAGE_BASED,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Fetches data asychronously from an API endpoint using the HTTP GET method.

        Args:
            self (Self): An instance of the class.
            plugin_id (str): The unique identifier of the plugin callabe. Often used for logging.
            base_url (str): The base URL of the API e.g. https://api.example.com/v1
            endpoint (str): The endpoint to fetch data from e.g. /users
            pagination_type (str, optional): The type of pagination strategy to use. Defaults to "page_based".
            headers (dict[str, str] | None, optional): An optional dict of headers. Defaults to None.

        """
        super().__init__(plugin_id)
        self.base_url = base_url
        self.endpoint = endpoint
        self.headers = headers
        self.pagination_strategy: PaginationStrategy = get_pagination_strategy(pagination_type)

    @staticmethod
    def _extract_data(response_data: dict | list) -> list[JSON_DATA]:
        """Extract data from response based on common patterns."""
        if isinstance(response_data, dict):
            # Standard REST API
            if "data" in response_data:
                return response_data["data"]
            return [response_data]

        if isinstance(response_data, list):
            # Direct list responses
            return response_data
        return []

    @retry(
        sleep=async_sleep,
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=4),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True,
    )
    async def __call__(self) -> list[JSON_DATA]:
        results = []
        next_page_url = f"{self.base_url}/{self.endpoint}"

        # Fetch API KEY securely
        api_key = os.getenv("API_KEY", "")  # TODO: This needs to be changeable such that AuthPlugin could be used.

        # Include API key in request headers
        default_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if self.headers:
            default_headers.update(self.headers)

        async with httpx.AsyncClient() as client:
            while next_page_url:
                response = await client.get(url=next_page_url, headers=default_headers)

                if response.status_code != HTTPStatus.OK:
                    logging.error("Failed to retrieve data. Status code: %s", response.status_code)
                    response.raise_for_status()

                response_json = response.json()
                results.extend(self._extract_data(response_json))

                # Handle Pagination
                next_page_url = (
                    self.pagination_strategy.get_next_page(response_json) if isinstance(response_json, dict) else None
                )

            return results
