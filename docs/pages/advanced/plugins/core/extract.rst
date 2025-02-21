.. _core_extract_plugins:

Extract Core Plugins
========================
``pipeline-flow`` provides a set of core extract plugins that enable you to fetch data from various source with ease.
Each plugin is designed to handle specific data extraction scenarios and can be easily integrated into your pipeline.


.. warning::
    Please refer to the :ref:`best practices <plugins_best_practices>` section to learn more about 
    how to use secrets in your pipeline configuration.


Below is a list of core extract plugins, their functionality, and configurable arguments.


API Plugins
------------------------------------
|br|

.. _rest_api_extractor:

Plugin: **rest_api_extractor**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``rest_api_extractor`` plugin enables you to fetch data asychronously from REST API endpoints using 
`httpx <https://www.python-httpx.org/>`_. It is a python library that provides a way to interac
with REST APIs in an asynchronous manner.

**Arguments:**  

.. list-table::
   :widths: 22 15 55 16  
   :header-rows: 1  

   * - **Argument**
     - **Data Type**
     - **Description**  
     - **Required** 
   * - `id`
     - str
     - Unique identifier for the plugin. It is used for logging and debugging purposes. If not provided, a random ID will be generated.
     - Optional
   * - `base_url`
     - str
     - Base URL of the REST API endpoint, e.g. ``https://api.example.com/v1/``.
     - Required 
   * - `endpoint`  
     - str
     - Endpoint path to fetch data from, e.g. ``/users``.
     - Required
   * - `headers`
     - dict
     - Headers to include in the API request. You must provide the necessary authentication headers here.
     - Required
   * - `pagination_type`
     - str
     - Type of pagination to use for fetching data. Supported values are ``page_based`` and ``hateoas``. Default is ``page_based``.
       If you want to learn more about :ref:`pagination plugins <core_pagination_handlers>`.
     - Optional

**Example Configuration:**  

.. code-block:: yaml

    extract:
      plugin: rest_api_extractor
      args:
        base_url: "https://api.example.com/v1"
        endpoint: "/users"
        headers:
          Authorization: "Bearer <token>" # or even better use secrets!!


.. |br| raw:: html

      <br>