.. _core_plugins:

Core Plugins
========================
``pipeline-flow`` comes with a set of core plugins that are available out of the box. 
These plugins are designed to cover common data processing needs.

.. warning::
    Unfortunately, as of now, there are not many built-in plugins available at the moment. 
    However, as the project grows, we will be adding more plugins to cover a wide range of use cases.

We will be adding plugins for common use cases, including:

- Database Connectors (MySQL, PostgreSQL, etc.)
- Cloud Storage Connectors (AWS S3, Google Cloud Storage, etc.)
- API Connectors (REST, GraphQL, etc.)
- Data Transformation Plugins (Pandas, PySpark, etc.)


.. _plugins_best_practices:

Best Practices
-----------------

.. note::
    It is highly recommended to **use secrets to store sensitive information** like API keys, passwords, etc.

    Please go to :ref:`secrets <secrets>` to learn more about how to use secrets in your pipeline configuration.

    Please go to :ref:`utility plugins <core_secret_plugins>` to learn more about how to use secret plugins.


Core Plugins List
-----------------
Here is a list of the core plugins available in the system:

.. toctree::
   :titlesonly:
   
   extract.rst
   transform.rst
   load.rst
   transform_load.rst
   utility.rst



Usage
-------------
To use a built-in plugin, you only need to specify the plugin name and its arguments in the pipeline configuration file. These
plugins are by default available in the system and do not require additional installation.

For example, in the yaml file below, you don't need to define it in the plugins section, but only within each individual phase step.
``EXAMPLE_PLUGIN`` is a placeholder for the plugin name you want to use.

.. code:: yaml

    plugins: ... # No need to define built-in plugins here
    
    pipelines:
      pipeline1:
        type: ETL # Define your pipeline type (ETL, ELT or ETLT)
        phases:
          extract:
            steps:
              - plugin: EXAMPLE_PLUGIN  # Just Specify the plugin name here:
                args: # And provide the parameters if required
                  param1: value1
                  param2: value2
              - plugin: ANOTHER_EXAMPLE_PLUGIN
                args:
                  param1: value1
                  param2: value2 
          transform:
            steps:
              - ...
          load:
            steps:
              - plugin: ...


Next Steps
-------------
- If you are interested in contributing to the core plugins, check out the :ref:`Plugin Development Guide <plugin_development>` to get started.
- Stay tuned for updates on new plugins and features in the upcoming releases.