# Standard Imports
import logging
import re
from http import HTTPStatus
from multiprocessing import Pipe
from typing import Any

# Third Party Imports
import httpx

# Local Imports
from pipeline_flow.common.type_def import AsyncPlugin
from pipeline_flow.core.models import PipelinePhase
from pipeline_flow.core.plugins import plugin

JSON_DATA = dict[str, Any]


@plugin(PipelinePhase.EXTRACT_PHASE, "async_get_httpx_paginated")
def async_get_httpx_paginated(
    api_key: str, base_url: str, endpoint: str, headers: dict[str, str] | None = None
) -> AsyncPlugin:
    """Fetches data asychronously from an API endpoint using the HTTP GET method.

    Args:
        api_key (str): An API key to authenticate the request.
        base_url (str): The base URL of the API e.g. https://api.example.com/v1
        endpoint (str): The endpoint to fetch data from e.g. /users
        headers (dict[str, str] | None, optional): A dict of headers. Defaults to None.

    Returns:
        AsyncPlugin: An inner async func that fetches data from the API endpoint.

    """

    async def inner() -> list[JSON_DATA]:
        results = []
        next_page_url = f"{base_url}/{endpoint}"

        # Include API key in request headers
        default_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if headers:
            default_headers.update(headers)

        async with httpx.AsyncClient() as client:
            while next_page_url:
                response = await client.get(
                    url=next_page_url,
                    headers=headers,
                )

                response.raise_for_status()

                # Extraction of data from the response
                if response.status_code != HTTPStatus.OK:
                    logging.error("Failed to retrieve data. Status code: %s", response.status_code)
                    return response

                response_json = response.json()
                if "data" in response_json:  # Standard REST API
                    results.extend(response_json["data"])
                elif isinstance(response_json, list):  # Direct list responses
                    results.extend(response_json)
                else:
                    results.append(response_json)

                # handle Pagination
                if "pagination" in response_json:
                    pagination = response_json["pagination"]
                    next_page_url = pagination.get("next_page") if pagination.get("has_more") else None
                else:
                    next_page_url = None

            return results

        logging.info("Failed to retrieve data. Status code: %s", response.status_code)
        return response

    return inner
