# Standad Imports

# Third Party Imports
import pytest
from httpx import HTTPStatusError
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

# Local Imports
from pipeline_flow.plugins import IPlugin
from pipeline_flow.plugins.extract import RestApiAsyncExtractor


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
    return RestApiAsyncExtractor(test_api_key, base_url, test_endpoint)


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
    httpx_mock.add_response(status_code=429, is_reusable=True)

    with pytest.raises(HTTPStatusError):
        await api_client()

    assert asyncio_sleep.call_count == 2, "The setting is set till 3 retries, so it should be 2"
