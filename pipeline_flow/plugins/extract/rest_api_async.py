# Standard Imports
import asyncio
import logging
from http import HTTPStatus
from typing import Any, Self

# Third Party Imports
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random

# Local Imports
from pipeline_flow.common.type_def import PluginPayload
from pipeline_flow.core.registry import PluginRegistry
from pipeline_flow.plugins import IExtractPlugin, IPaginationHandler

JSON_DATA = dict[str, Any]


async def async_sleep(seconds: float) -> None:
    logging.debug("Retrying in %s seconds", seconds)
    await asyncio.sleep(seconds)


class RestApiAsyncExtractor(IExtractPlugin, plugin_name="rest_api_extractor"):
    """Fetches data asychronously from an API endpoint using the HTTP GET method.

    Args:
        plugin_id (str): The unique identifier of the plugin callabe. Often used for logging.
        base_url (str): The base URL of the API e.g. https://api.example.com/v1
        endpoint (str): The endpoint to fetch data from e.g. /users
        headers (dict[str, str]): A dictionary that contains headers e.g. auth token.
        pagination (PluginPayload, optional): A dict that contains plugins and args for paginations. Defaults to "page_based_pagination" plugin.
    """

    def __init__(
        self: Self,
        plugin_id: str,
        base_url: str,
        endpoint: str,
        headers: dict[str, str],
        pagination: PluginPayload | None = None,
    ) -> None:
        super().__init__(plugin_id)
        self.base_url = base_url
        self.endpoint = endpoint
        self.headers = headers

        # Fetches the pagination plugin from the registry
        # if no pagination plugin is provided, the default is "page_based".
        pagination_payload = pagination or {
            "id": f"{plugin_id}_pagination",
            "plugin": "page_based_pagination",
        }

        self.pagination_handler: IPaginationHandler = PluginRegistry.instantiate_plugin(pagination_payload)

        if not headers:
            raise ValueError("Headers must be provided for the API request.")

    @staticmethod
    def _extract_data(response_data: dict | list) -> list[JSON_DATA]:
        """Extracts data from the response JSON.

        This method handles the different ways data can be returned from an API.

        Args:
            response_data (dict | list):  The response JSON data.

        Returns:
            list[JSON_DATA]: Parsed data from the response JSON.
        """
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
        """Fetches data from the API endpoin asynchronously.

        Returns:
            list[JSON_DATA]: The extracted data from the API.
        """
        # TODO: Add supports for multiple endpoints with paginations async.
        results = []
        next_page_url = f"{self.base_url}/{self.endpoint}"

        # Include API key in request headers
        default_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

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
                next_page_url = self.pagination_handler(response_json) if isinstance(response_json, dict) else None

            return results
