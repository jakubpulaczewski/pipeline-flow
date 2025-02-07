# Standard Imports

# Third Party Imports
import pytest
from pytest_mock import MockerFixture, MockType

# Project Imports
from pipeline_flow.core.parsers import PluginParser


@pytest.fixture
def mock_isdir(mocker: MockerFixture) -> MockType:
    return mocker.patch("os.path.isdir", return_value=False)


@pytest.mark.usefixtures("mock_isdir")
def test_get_all_files_with_empty_input() -> None:
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]
    assert plugin_parser.get_all_files([]) == set()


@pytest.mark.usefixtures("mock_isdir")
def test_get_all_files_with_no_valid_files() -> None:
    paths = ["file1.txt", "file2.log"]
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]

    result = plugin_parser.get_all_files(paths)

    assert result == set()


@pytest.mark.usefixtures("mock_isdir")
def test_get_all_files_with_only_files() -> None:
    paths = ["file1.py", "file2.txt", "file3.py"]
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]

    result = plugin_parser.get_all_files(paths)

    assert result == {"file1.py", "file3.py"}


@pytest.mark.usefixtures("mock_isdir")
def test_get_all_files_with_only_duplicates() -> None:
    paths = ["file1.py", "file3.py", "file3.py", "file3.py"]
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]

    result = plugin_parser.get_all_files(paths)

    assert result == {"file1.py", "file3.py"}


def test_get_all_files_with_directories_and_files(mocker: MockerFixture) -> None:
    mock_isdir = mocker.patch("os.path.isdir")
    mock_listdir = mocker.patch("os.listdir")

    # Define mock behavior
    mock_isdir.side_effect = lambda path: path == "dir1"
    mock_listdir.return_value = ["file1.py", "file2.txt", "file3.py"]

    paths = ["dir1", "file4.py", "file5.txt"]
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]

    result = plugin_parser.get_all_files(paths)
    expected = {"dir1/file1.py", "dir1/file3.py", "file4.py"}
    assert result == expected


def test_fetch_custom_plugin_files_with_no_dirs_or_files(mocker: MockerFixture) -> None:
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]

    mocker.patch.object(plugin_parser, "get_all_files", side_effect=[set(), set()])

    custom_plugins = plugin_parser.fetch_custom_plugin_files()

    assert custom_plugins == set()


def test_fetch_custom_plugin_files_with_dirs_and_files(mocker: MockerFixture) -> None:
    plugin_parser = PluginParser(plugins_payload={"custom": {"dirs": ["dir1"], "files": ["file1.py", "file2.py"]}})  # type: ignore[reportArgumentType]
    files_mock = mocker.patch.object(
        plugin_parser,
        "get_all_files",
        side_effect=[
            {"dir1/fileA.py", "dir2/fileB.py"},  # Mocked output for directories
            {"file1.py", "file2.py"},  # Mocked output for files
        ],
    )

    custom_plugins = plugin_parser.fetch_custom_plugin_files()

    files_mock.assert_any_call(["dir1"])
    files_mock.assert_any_call(["file1.py", "file2.py"])

    assert custom_plugins == {
        "file2.py",
        "dir2/fileB.py",
        "file1.py",
        "dir1/fileA.py",
    }


def test_fetch_custom_plugin_files_with_only_files(mocker: MockerFixture) -> None:
    plugin_parser = PluginParser(plugins_payload={"custom": {"files": ["file1.py", "file2.py"]}})  # type: ignore[reportArgumentType]

    files_mock = mocker.patch.object(
        plugin_parser,
        "get_all_files",
        side_effect=[
            set(),  # Mocked output for directories
            {"file1.py", "file2.py"},  # Mocked output for files
        ],
    )

    custom_plugins = plugin_parser.fetch_custom_plugin_files()
    files_mock.assert_any_call(["file1.py", "file2.py"])

    assert custom_plugins == {"file2.py", "file1.py"}


def test_fetch_custom_plugin_files_with_overlapping_files(mocker: MockerFixture) -> None:
    plugin_parser = PluginParser(
        plugins_payload={  # type: ignore[reportArgumentType]
            "custom": {
                "dirs": ["dir1", "dir2"],
                "files": ["dir1/fileA.py", "dir2/fileB.py"],
            }
        }
    )

    files_mock = mocker.patch.object(
        plugin_parser,
        "get_all_files",
        side_effect=[
            {"dir1/fileA.py", "dir2/fileB.py"},
            {"dir1/fileA.py", "dir2/fileB.py"},
        ],
    )

    custom_plugins = plugin_parser.fetch_custom_plugin_files()

    files_mock.assert_any_call(["dir1", "dir2"])
    files_mock.assert_any_call(["dir1/fileA.py", "dir2/fileB.py"])

    assert custom_plugins == {"dir1/fileA.py", "dir2/fileB.py"}


def test_fetch_community_plugin_modules_empty() -> None:
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]

    assert plugin_parser.fetch_community_plugin_modules() == set()


def test_fetch_community_plugin_modules_success() -> None:
    plugin_parser = PluginParser(plugins_payload={"community": ["plugin1", "plugin2", "plugin3"]})  # type: ignore[reportArgumentType]

    result = plugin_parser.fetch_community_plugin_modules()

    assert result == {
        "community.plugins.plugin1",
        "community.plugins.plugin2",
        "community.plugins.plugin3",
    }
