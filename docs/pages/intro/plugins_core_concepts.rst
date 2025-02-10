.. _plugin_core_concepts:

Plugins
=======
``pipeline-flow`` follows a **plugin-based architecture** that allows users to extend the system's functionality by adding custom plugins. 
Plugins are reusable components that encapsulate specific functionality and can be easily integrated into the system.


What exactly is a Plugin?
--------------------------
A plugin is:

#. Any callable Python object that implements the ``__call__`` dunder method. In other words, a plugin is a function or a class with a ``__call__`` method.
#. Wrapped in a decorator that enables lazy initialization of the plugin and allows the use of custom arguments in addition to arguments specific to a given phase.

Why use Plugins?
------------------
Plugins offer several benefits:

- **Reusability**: Plugins can be reused across different pipelines, reducing code duplication.
- **Modularity**: Each plugin is a self-contained unit of functionality, making it easier to maintain and test.
- **Extensibility**: Users can easily extend the system's functionality by adding custom plugins without modifying the core codebase.
- **Flexibility**: Plugins can be added or updated dynamically, enabling rapid development and deployment.

Types of Plugins
-----------------
There are two types of plugins in ``pipeline-flow``:

- **Synchronous Plugins**: Plugins that execute synchronously and are suitable for CPU-bound operations (e.g. Transform and Transform-Load plugins).
- **Asynchronous Plugins**: Plugins that execute asynchronously and are suitable for I/O-bound operations (e.g. Extract and Load plugins).

For example, the following code snippet demonstrates a simple plugin that converts the string input to uppercase, and
another plugin that performs an I/O operation:

.. code:: python

  >>> from functools import wraps
  >>> from pipeline_flow.common.type_def import AsyncPlugin, SyncPlugin
  >>> from pipeline_flow.core.plugins import plugin

Asynchronous Plugin:
  >>> def async_plugin() -> AsyncPlugin:
  >>>   @wraps(custom_plugin)
  >>>   async def inner(data: str, arg1, arg2) -> None:
  >>>       # Perform some I/O operation
  >>>       return  # and return it !
  >>>   return inner

Synchronous Plugin:
  >>> def sync_plugin() -> SyncPlugin:
  >>>   @wraps(custom_plugin)
  >>>   def inner(data: str, arg1, arg2) -> None:
  >>>       return data.upper() # Do something with the data
  >>>   return inner


Next Steps
-----------------
- Check out the :ref:`Pipeline Orchestration <core_concepts_pipeline_orchestration>` guide to learn how to manage pipeline dependencies and their execution order.
- Explore the :ref:`Plugin <plugins>` guide to learn more about plugins, e.g. how to create and use them.