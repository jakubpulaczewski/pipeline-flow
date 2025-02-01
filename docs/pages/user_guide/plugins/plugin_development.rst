.. _plugin_development:

Plugin Development Guide
========================
Plugins are the building blocks of ``pipeline-flow`` that enable users to meet their specific
data processing needs.

Plugins follow a standardised structure and can be easily integrated into the system. This guide will help you
understand the plugin development process and build custom plugins for your pipelines.

Creating a Plugin
-----------------

1. **Plugin Structure**

A plugin is any Python function class that is wrapped in a decorator. It is not required but recommend to include id
as a function argument to uniquely identify the plugin. If you do not provide an id, the system will generate an
unique identifier for the plugin based on the outer function name.

If you would like to add additional arguments to the plugin itself, you can do so by adding them to the inner function.

For example, the following code snippet demonstrates a simple plugin that converts the input string to uppercase:


.. code:: python

  >>> from functools import wraps
  >>> from pipeline_orchestrator.common.type_def import AsyncPlugin, SyncPlugin
  >>> from pipeline_orchestrator.core.plugins import plugin

Asynchronous Plugin:
  >>> def async_plugin(id: str) -> AsyncPlugin:
  >>>   @wraps(custom_plugin)
  >>>   async def inner(data: str, arg1, arg2) -> None:
  >>>       return data.upper()
  >>>   return inner

Synchronous Plugin:
  >>> def sync_plugin(id: str) -> SyncPlugin:
  >>>   @wraps(custom_plugin)
  >>>   async def inner(data: str, arg1, arg2) -> None:
  >>>       return data.upper()
  >>>   return inner

2. **Register the Plugin**:
To dynamically register a plugin with the system, use the `plugin` decorator. This will allow the system to identify and load the plugin when needed.
In the decorator, specify the phase in which the plugin will be executed and optionally the plugin name, if different from the function name.

For example, to register the plugin created in the previous step:


.. code:: python

  >>> from pipeline_orchestrator.core.models import PipelinePhase
  >>> @plugin(PipelinePhase.EXTRACT_PHASE, "custom_plugin")
  >>> def custom_plugin(id: str) -> SyncPlugin:
  >>>     @wraps(custom_plugin)
  >>>     async def inner(data: str) -> None:
  >>>         return data.upper()
  >>>     return inner


Using Your Plugin
-----------------
After creating and registering your plugin, you can use it in your pipeline configuration file.
If it is a custom plugin, ensure that you are using ''custom'' when defining the plugin in the pipeline configuration file.
You can either specify the directory where the plugin is located or provide the plugin file name.

For example, to use the custom plugin created in the previous steps:

.. code:: yaml

    plugins:
      custom:
        dirs:
          - /path/to/custom/plugins
        files:
          - custom_plugin.py
    pipelines:
      pipeline1:
        # Define your pipeline configuration here


Alternatively, to use community plugins, you can specify the plugin name directly in the pipeline configuration file.
For example, to use the `api_connector_example` plugin:

.. code:: yaml

    plugins:
      community:
        - api_connector_example
    pipelines:
      pipeline1:
        # Define your pipeline configuration here


Best Practices
-----------------
- Follow Naming Conventions: Ensure your plugin name is descriptive and unique.
- Use Descriptive Arguments: Use meaningful names for arguments to make the plugin more readable.
- Document Your Plugin: Include comments and docstrings to explain the purpose and functionality of the plugin.
- Test Your Plugin: Write unit tests to validate the plugin's functionality and ensure it works as expected.
- Share Your Plugin: Consider sharing your plugin with the community by contributing to the official plugin repository.


Sharing Your Plugin
-------------------
Once your plugin is ready, consider sharing it with the community by contributing to the 
`official plugin repository <https://github.com/jakubpulaczewski/pipeline-flow-community>`_.



Next Steps
-----------
- Explore more :ref:`built-in plugins <core_plugins>` for inspiration.

Happy coding! 🚀