.. _plugins:

Plugins
=======
Plugins extend the functionality of ``pipeline-orchestrator``, allowing users to integrate with 
different data sources, apply custom transformations, and enhance pipeline capabilities without 
modifying the core system.

Built-in Plugins
----------------
As of now, there are no ref:`built-in plugins <core_plugins>` available. However, as the project grows, we will add more plugins to the core system.
We will be adding plugins for common use cases, including:


- Database Connectors (MySQL, PostgreSQL, etc.)
- Cloud Storage Connectors (AWS S3, Google Cloud Storage, etc.)
- API Connectors (REST, GraphQL, etc.)
- Data Transformation Plugins (Pandas, PySpark, etc.)

To use a built-in plugin, you only need to specify the plugin name and its parameters in the pipeline configuration file. These
plugins are by default available in the system and do not require additional installation.

Community Plugins
-----------------
The community can contribute plugins to the ``pipeline-orchestrator`` ecosystem. Community plugins can be shared with others 
and used in different pipelines. To use a community plugin, you need to install the plugin package using a package manager like
pip or poetry.

It is worth noting that community plugins are not part of the core system and need to be imported explicitly, 
as well defined in the yaml configuration file.

.. code:: bash

  pip install 'pipeline-orchestrator-community[PLUGIN_NAME]'
  
where ``PLUGIN_NAME`` is the name of the plugin defined as an extra dependency in the plugin package.

For a list of available plugins, visit the `Community Plugin Repository <https://github.com/jakubpulaczewski/pipeline-orchestrator-community>`_.

Custom Plugins
--------------
For advanced use cases, you can build custom plugins to extend the functionality. Custom plugins enable:

- Custom Data Extraction: Connect to APIs, web services, or proprietary data sources.
- Advanced Transformations: Implement business-specific data processing logic.
- Specialized Load Mechanisms: Load data into unique storage formats or non-standard databases.


Next Steps
-------------
- Explore the Plugin Development Guide to build your own :ref:`custom plugins <plugin_development>`.
- Contribute to the Community Plugin Repository and help others build better pipelines.
