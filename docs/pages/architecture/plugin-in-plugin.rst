.. _plugin_in_plugin:


Plugin-in-Plugin: Extending Functionality
=========================================================
The ``pipeline-flow`` framework is designed with extensibility in mind, allowing you to build upon existing plugins by defining
interfaces that other plugins can implement. 

We call this technique `"Plugin-in-Plugin"` which enables to act as a modular building blocks - much like LEGO pieces - that can 
be combined to create more complex and powerful functionalities.

These plugins follow the :ref:`same plugin structure <plugin_yaml_structure>` as regular plugins, but are defined as arguments to other plugins.



**Example Scenario**:

Imagine you have a plugin that extracts data from a REST API. It works well until you encounter an API with a different pagination mechanism that the plugin
doesn't support. One option would be to tightly couple the pagination logic directly into the plugin. However, this would reduce flexibility and make future 
maintenance more difficult.

A better approach would be to create an abstraction for pagination logic, which can be used by any plugin that requires pagination. This way, you can customise
the pagination mechanism without affecting the main plugin, while extending its functionality. This adheres to the SOLID principles and make you code more
modular and easier to maintain. 

This means you can reuse the same pagination handler with multiple plugins, making your code more modular and easier to maintain.


.. note:: Future Enhancements
    We are working on expanding the ``plugin-in-plugin`` technique to support more complex scenarios.

    One key enhancement is to enable each phase of the pipeline, such as extract or transform, to support pre and post processing steps. This will allow you to define
    custom logic that can be executed before or after the main plugin logic, providing more flexibility and control over the pipeline execution.

    For example, after extracting data from a source, you may want to store in S3, like you would store raw data in a bronze layer in a data lake. You could define
    a post-processing step that would handle this task, ensuring that the data is stored correctly after extraction, while still keeping the pipeline running the main
    logic, e.g. transforming data asynchronously, in another thread.
