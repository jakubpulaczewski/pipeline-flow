# # Third Party
# import pytest
# from faker import Faker
# from pytest_cases import parametrize_with_cases

# # Project
# import common.config as config  # pylint: disable=consider-using-from-import
# from core.plugins import PluginFactory
# from tests import test_utils

# fake = Faker()


# # def create_class_for_etl_stage(stage: str) -> type:
# #     stage = stage.lower()

# #     if stage == "extract":

# #         def extract(self) -> str:
# #             return "Extracting data"

# #         return type("DynamicExtractor", (), {"extract": extract})

# #     elif stage == "transform":

# #         def transform(self, data) -> str:
# #             return "Transforming data"

# #         return type("DynamicTransformer", (), {"transform": transform})

# #     elif stage == "load":

# #         def load(self, data) -> str:
# #             return "Loading data"

# #         return type("DynamicLoader", (), {"load": load})


# # def create_generic_etl_class():
# #     return type("Generic", (), {})


# # @pytest.fixture()
# # def clean_plugin_registry():
# #     """A pytest fixture to reset the plugin map."""
# #     yield
# #     # Teardown code runs after each test function that uses the fixture
# #     PluginFactory._plugin_map = {}  # pylint: disable=protected-access


# @pytest.mark.parametrize("etl_stage", config.ETL_STAGES)
# def case_plugin_factory(
#     etl_stage, clean_plugin_registry
# ):  # pylint: disable=redefined-outer-name,unused-argument
#     """A test case returning etl stage, plugin name, and class associated with the plugin."""
#     plugin = test_utils.generate_plugin_name()
#     plugin_cls = test_utils.create_fake_class(plugin)
#     return etl_stage, plugin, plugin_cls


# @parametrize_with_cases("etl_stage, plugin, plugin_cls", cases=".")
# def test_register_plugin(etl_stage: str, plugin: str, plugin_cls) -> None:
#     """A test that validates the `register` functionality of the PluginFactory."""
#     # Register the plugin
#     result = PluginFactory.register(etl_stage, plugin, plugin_cls)
#     assert result is True

#     # Try to register the same plugin again, should return False
#     result = PluginFactory.register(etl_stage, plugin, plugin_cls)
#     assert result is False


# @parametrize_with_cases("etl_stage, plugin, plugin_cls", cases=".")
# def test_remove_plugin(etl_stage: str, plugin: str, plugin_cls) -> None:
#     """A test that validates the `removal` functionality of the PluginFactory."""
#     # Add the plugin
#     PluginFactory.register(etl_stage, plugin, plugin_cls)

#     # Removes the plugin
#     result = PluginFactory.remove(etl_stage, plugin)
#     assert result is True

#     # Try to remove the same plugin, should return False
#     result = PluginFactory.remove(etl_stage, plugin)
#     assert result is False


# @parametrize_with_cases("etl_stage, plugin, plugin_cls", cases=".")
# def test_get_plugin(etl_stage: str, plugin: str, plugin_cls) -> None:
#     """A test that validates the `getting` functionality of the PluginFactory."""

#     # Add the plugin
#     PluginFactory.register(etl_stage, plugin, plugin_cls)

#     # Get the plugin
#     result = PluginFactory.get(etl_stage, plugin)
#     assert result is plugin_cls

#     # Try to get the same plugin plugin, after its removed, should return None
#     PluginFactory.remove(etl_stage, plugin)

#     result = PluginFactory.get(etl_stage, plugin)
#     assert result is None


# # @pytest.mark.parametrize("etl_stage", config.ETL_STAGES)
# # def test_validate_plugin_interface_success(etl_stage):
# #     plugin_class = create_class_for_etl_stage(etl_stage)

# #     assert (
# #         PluginFactory._validate_plugin_interface(etl_stage.upper(), plugin_class)
# #         is True
# #     )


# # @pytest.mark.parametrize(
# #     "etl_stage", ["wrong_extract", "wrong_transform", "wrong_load"]
# # )
# # def test_validate_plugin_interface_type_failure(etl_stage):
# #     plugin_class = create_generic_etl_class()

# #     with pytest.raises(TypeError):
# #         PluginFactory._validate_plugin_interface(etl_stage.upper(), plugin_class)


# # def test_validate_plugin_interface_not_subclass_failure():
# #     plugin_class = create_generic_etl_class()

# #     PluginFactory._validate_plugin_interface("extract", plugin_class)
