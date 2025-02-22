# Standad Imports
from typing import Generator

# Third Party Imports
import pytest
from httpx import HTTPStatusError
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from pipeline_flow.core.parsers import YamlParser

# Local Imports
from pipeline_flow.core.registry import PluginRegistry
from pipeline_flow.plugins import IPlugin
from pipeline_flow.plugins.extract import RestApiAsyncExtractor
from pipeline_flow.plugins.utility import pagination


@pytest.fixture
def test_api_key() -> str:
    return "test_api_key"


@pytest.fixture
def base_url() -> str:
    return "https://api.example.com/v1"


@pytest.fixture
def test_endpoint() -> str:
    return "/users"


@pytest.fixture
def api_client(test_api_key: str, base_url: str, test_endpoint: str) -> IPlugin:
    return RestApiAsyncExtractor(
        plugin_id="test_api_extractor",
        base_url=base_url,
        endpoint=test_endpoint,
        headers={"Authorization": test_api_key},
    )


@pytest.fixture(autouse=True)
def register_plugins_in_registry(restart_plugin_registry) -> Generator[None]:  # noqa: ARG001 - A fixture is being used.
    # When running the tests, the plugins are already registered in the registry.
    # However, they are overriden by other tests. This fixture is used to re-register the plugins.
    PluginRegistry.register("rest_api_extractor", RestApiAsyncExtractor)
    PluginRegistry.register("hateoas_pagination", pagination.HATEOASPagination)
    PluginRegistry.register("page_based_pagination", pagination.PageBasedPagination)


@pytest.mark.asyncio
async def test_direct_list_response(api_client: IPlugin, httpx_mock: HTTPXMock) -> None:
    json = [{"id": 1, "name": "Single Item"}]
    httpx_mock.add_response(status_code=200, json=json)

    result = await api_client()

    assert result == [{"id": 1, "name": "Single Item"}]


@pytest.mark.asyncio
async def test_direct_dict_response(api_client: IPlugin, httpx_mock: HTTPXMock) -> None:
    json = {"id": 1, "name": "Single Item"}
    httpx_mock.add_response(status_code=200, json=json)

    result = await api_client()

    assert result == [{"id": 1, "name": "Single Item"}]


@pytest.mark.asyncio
async def test_pagination_single_page(api_client: IPlugin, httpx_mock: HTTPXMock) -> None:
    json = {"data": [{"id": 1, "name": "Single Item"}], "pagination": {"has_more": False, "next_page": None}}

    httpx_mock.add_response(status_code=200, json=json)

    result = await api_client()

    assert result == [{"id": 1, "name": "Single Item"}]


@pytest.mark.asyncio
async def test_pagination_multiple_pages(api_client: IPlugin, httpx_mock: HTTPXMock) -> None:
    first_page = {
        "data": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
        "pagination": {"has_more": True, "next_page": "https://api.example.com/v1/users?page=2"},
    }
    second_page = {
        "data": [{"id": 3, "name": "Item 3"}],
        "pagination": {"has_more": False, "next_page": None},
    }
    httpx_mock.add_response(status_code=200, json=first_page)
    httpx_mock.add_response(status_code=200, json=second_page)

    result = await api_client()

    assert result == [
        {"id": 1, "name": "Item 1"},
        {"id": 2, "name": "Item 2"},
        {"id": 3, "name": "Item 3"},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [(403), (404), (429), (500), (502), (503), (504)])
async def test_api_failure(status_code: int, api_client: IPlugin, httpx_mock: HTTPXMock, mocker: MockerFixture) -> None:
    asyncio_sleep = mocker.patch("asyncio.sleep")
    httpx_mock.add_response(status_code=status_code, is_reusable=True)

    with pytest.raises(HTTPStatusError):
        await api_client()

    assert asyncio_sleep.call_count == 2, "The setting is set till 3 retries, so it should be 2"


def test_parse_rest_api_extractor_with_different_pagination_handler(
    base_url: str, test_endpoint: str, test_api_key: str
) -> None:
    yaml_config = f"""
    extract:
      steps:
        - plugin: rest_api_extractor
          args:
            base_url: {base_url}
            endpoint: {test_endpoint}
            headers:
              Authorization: {test_api_key}
            pagination:
              plugin: hateoas_pagination
    """

    # Parse the YAML configuration
    parsed_yaml = YamlParser(stream=yaml_config).content
    extract_step = parsed_yaml["extract"]["steps"][0]

    # Instantiate the plugin (plugin_id is assigned by `instantiate_plugin` in PluginRegistry)
    extractor = RestApiAsyncExtractor(plugin_id="test_plugin", **extract_step["args"])

    assert isinstance(extractor, RestApiAsyncExtractor), "Extractor instance is not of type RestApiAsyncExtractor"
    assert isinstance(extractor.pagination_handler, pagination.HATEOASPagination), (
        "Pagination handler is not HateoasPagination"
    )


def test_parse_rest_api_extractor_yaml(base_url: str, test_endpoint: str, test_api_key: str) -> None:
    yaml_config = f"""
    extract:
      steps:
        - plugin: rest_api_extractor
          args:
            base_url: {base_url}
            endpoint: {test_endpoint}
            headers:
              Authorization: {test_api_key}
    """

    # Parse the YAML configuration
    parsed_yaml = YamlParser(stream=yaml_config).content
    extract_step = parsed_yaml["extract"]["steps"][0]

    # Assert that the plugin is correctly parsed
    assert extract_step["plugin"] == "rest_api_extractor", "Plugin name did not match 'rest_api_extractor'"

    # Instantiate the plugin (plugin_id is assigned by `instantiate_plugin` in PluginRegistry)
    extractor = RestApiAsyncExtractor(plugin_id="test_plugin", **extract_step["args"])

    # Verify that the instantiated object is of the correct type
    assert isinstance(extractor, RestApiAsyncExtractor), "Extractor instance is not of type RestApiAsyncExtractor"
