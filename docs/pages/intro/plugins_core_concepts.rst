.. _plugin_core_concepts:

Plugins
=======
``pipeline-flow`` follows a **plugin-based architecture** that allows users to extend the system's functionality by adding custom plugins. 
Plugins are reusable components that encapsulate specific functionality and can be easily integrated into the system.


What exactly is a Plugin?
--------------------------
In ``pipeline-flow``, a plugin is a Python class that implements a specific interface from already defined interfaces. 
All of these classes behave like functions as they implement the ``__call__`` method. This method is the entry point for the plugin's execution.


Why use Plugins?
------------------
Plugins offer several benefits:

- **Reusability**: Plugins can be reused across different pipelines, reducing code duplication.
- **Modularity**: Each plugin is a self-contained unit of functionality, making it easier to maintain and test.
- **Extensibility**: Users can easily extend the system's functionality by adding custom plugins without modifying the core codebase.
- **Flexibility**: Plugins can be added or updated dynamically, enabling rapid development and deployment.

Plugin Registry
----------------
``pipeline-flow`` uses a plugin registry to act as a central repository to register and access plugins. This registry implements
a thread-safe singleton pattern to ensure that plugins are registered and accessed correctly. It is important in the simplest terms
its a dictionary that maps plugin names to their corresponding functions, and therefore each plugin needs to have a unique name
when creating it, otherwise it will raise an error.


Types of Plugins
-----------------
There are two types of plugins in ``pipeline-flow``:

- **Synchronous Plugins**: Plugins that execute synchronously and are suitable for CPU-bound operations (e.g. Transform and Transform-Load plugins).
- **Asynchronous Plugins**: Plugins that execute asynchronously and are suitable for I/O-bound operations (e.g. Extract and Load plugins).

If you want to find out more about how to create and use plugins, check out the :ref:`Plugin <plugins>` guide.

Next Steps
-----------------
- Check out the :ref:`Pipeline Orchestration <core_concepts_pipeline_orchestration>` guide to learn how to manage pipeline dependencies and their execution order.
- Explore the :ref:`Plugin <plugins>` guide to learn more about plugins, e.g. how to create and use them.