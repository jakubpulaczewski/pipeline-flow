.. _plugin_development:

Plugin Development
========================
Plugins are the **building blocks** of ``pipeline-flow`` enalbing users to meet their specific data processing needs.
They follow a **standardised structure** and can be easily integrated into the system.

This guide will walk you tthrough the **plugin development process* and help you build **custom plugins**
for your pipelines.

Creating a Plugin
-----------------

1. **Plugin Structure**

A ``pipeline-flow`` plugin is a self-contained component designed to perform a specific task within the framework, thereby extending its functionality.


Each plugin must implement a specific interface, allowing the core systme to validate and enforce the correct plugin types during pipeline execution.
Once created, the plugin is automatically registered with the system, making it available via the pipeline registry.


For an overview of available plugin types and their interfaces, refer to the :ref:`plugin hierarchy <plugin_hierarchy>`.

All plugin interfaces are defined in the `following file <https://github.com/jakubpulaczewski/pipeline-flow/blob/main/pipeline_flow/plugins/base.py>`_.

|br|

2. **Plugin Requirements**
At its core, every plugin must implement ``__call__`` dunder method, which:

- Acts as the main execution point of the plugin.
- Receives a specific set of arguments defined by the plugin interface.
- Allows the plugin to behave like a function while encapsulating internal state and additional helper methods within the class.

**Example use cases:**

- An IExtractPlugin does not require any arguments in the ``__call__`` method, focusing on data extraction from predefined source. But 
  it might require additional arguments in the constructor to define the source properties.
- An ITransformPlugin accepts extracted data as an argument in the ``__call__`` method, focusing on data transformation, extracted from
  the previous phase.



|br|


3. **Building a Plugin**

Creating a plugin in ``pipeline-flow`` is **super simple**—it only involves **two steps**.

1. Implement the specific plugin interface.
2. Define the plugin logic. That's it! |:tada:| 

**Example**:


Let's create a simple plugin that extracts data from any source.

.. code:: python

  >>> from pipeline_flow.plugins import IExtractPlugin
  >>>
  >>> class CustomExtractPlugin(IExtractPlugin):
  >>>
  >>>     def __init__(self, plugin_id: str, source: str) -> None:
  >>>        super().__init__(plugin_id) 
  >>>           self.source = source
  >>>
  >>>     def __call__(self):
  >>>        # Stimulate data extraction from the source
  >>>        return "Data extracted successfully!"


**Explanation**:

- **`__init__`:** Initializes the plugin with a `plugin_id` and a `source`.  
- **`__call__`:** Contains the core extraction logic, returning a success message.  
- The plugin **acts like a function** while maintaining internal state through the class.  


2. **Register the Plugin**:

Plugins in ``pipeline-flow`` are registered automatically when they implement the plugin interface. However, you need 
to ensure that your python file is imported (if it is in a separate file).

.. note::
    Registration is handled through the ``__init_subclass__`` dunder method, which initializes all subclasses of the plugin interface
    and registers them to the plugin registry.

    This enables no manual registration of plugins, making the process seamless and efficient.


Using Your Plugin
-----------------
After creating and registering your plugin, you can use it in your pipeline configuration file!!

If it is a custom plugin, ensure that you are using ''custom'' when defining the plugin in the pipeline configuration file.
You can either specify the directory where the plugin is located or provide the plugin file name.


The benefits of using a ``dirs`` over ``files`` is that it allows you to import multiple plugins from the same directory.
However, if you have a single plugin file, you can use the ``files`` option.


**Custom Plugin**:

For example, to use the custom plugin created in the previous steps:

.. code:: yaml

    plugins:
      custom:
        dirs:
          - /path/to/custom/plugins  # Directory where the custom plugin is located
        files:
          - custom_plugin.py # Or the file name where the custom plugin is defined
    pipelines:
      pipeline1:
        # Define your pipeline configuration here


** Community Plugin**:

Alternatively, to use community plugins, you can specify the plugin name directly in the pipeline configuration file.
For example, to use the `api_connector_example` plugin:

.. code:: yaml

    plugins:
      community:
        - api_connector_example
    pipelines:
      pipeline1:
        # Define your pipeline configuration here

** Built-in Plugin**:
For built-in plugins, you don't have to specify the plugin name in the ``plugins`` section, as they are available by default in the system.
Instead, you can just use the plugin name directly in the pipeline configuration file.


Best Practices
-----------------
- Follow Naming Conventions: Ensure your plugin name is descriptive and unique.
- Use Descriptive Arguments: Use meaningful names for arguments to make the plugin more readable.
- Document Your Plugin: Include comments and docstrings to explain the purpose and functionality of the plugin.
- Test Your Plugin: Write unit tests to validate the plugin's functionality and ensure it works as expected.
- Share Your Plugin: Consider sharing your plugin withP the community by contributing to the official plugin repository.


Sharing Your Plugin
-------------------
Once your plugin is ready, consider sharing it with the community by contributing to the 
`official plugin repository <https://github.com/jakubpulaczewski/pipeline-flow-community>`_.


Happy coding! |:rocket:|


.. |br| raw:: html

      <br>