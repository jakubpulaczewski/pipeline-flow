# Standard Imports
import shutil
from pathlib import Path
from typing import Generator

# Third Party Imports
import pytest

# Project Imports
from core.parsers import PluginParser


@pytest.fixture
def create_temp_plugin_folder() -> Generator[Path]:
    parent_path = Path(__file__).parent.parent

    plugins_dir = parent_path / "resources" / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    assert plugins_dir.is_dir()

    yield plugins_dir

    # Cleanup
    if plugins_dir.exists():
        shutil.rmtree(plugins_dir, ignore_errors=True)


def test_fetch_custom_plugin_files_with_only_files() -> None:
    plugins_payload = {
        "custom": {
            "files": [
                "tests/resources/plugins/custom_extractor.py",
                "tests/resources/plugins/custom_loader.py",
            ]
        }
    }

    plugin_parser = PluginParser(plugins_payload)  # type: ignore[reportArgumentType]

    result = plugin_parser.fetch_custom_plugin_files()

    expected = {
        "tests/resources/plugins/custom_extractor.py",
        "tests/resources/plugins/custom_loader.py",
    }

    assert result == expected


def test_fetch_custom_plugins_with_not_found_folder() -> None:
    # Create a temp folder
    plugins_payload = {
        "custom": {
            "dirs": [
                "tests/resources/plugins_not_found",
            ]
        }
    }
    plugin_parser = PluginParser(plugins_payload)  # type: ignore[reportArgumentType]

    assert plugin_parser is not None

    result = plugin_parser.fetch_custom_plugin_files()

    assert result == set()


def test_fetch_custom_plugin_files_with_dirs_and_files(create_temp_plugin_folder: Generator[Path]) -> None:
    # Create plugin
    file_path = create_temp_plugin_folder / "plugin123.py"  # type: ignore[reportOperatorIssue]
    file_path.write_text("")
    assert file_path.is_file()

    plugins_payload = {
        "custom": {
            "dirs": [
                "tests/resources/plugins",
            ],
            "files": [
                "tests/resources/plugins/custom_plugins.py",
            ],
        }
    }

    plugin_parser = PluginParser(plugins_payload)  # type: ignore[reportArgumentType]

    result = plugin_parser.fetch_custom_plugin_files()

    expected = {
        "tests/resources/plugins/plugin123.py",
        "tests/resources/plugins/custom_plugins.py",
    }

    assert result == expected


def test_get_custom_plugin_files_empty() -> None:
    plugin_parser = PluginParser(plugins_payload={})  # type: ignore[reportArgumentType]
    result = plugin_parser.fetch_custom_plugin_files()

    assert result == set()
