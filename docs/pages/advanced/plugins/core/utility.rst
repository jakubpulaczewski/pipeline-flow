.. _core_utility_plugins:

Utility Plugins
========================
Utility Plugins are based on the concept of :ref:`plugin-in-plugin <plugin_in_plugin>` and are designed to further extend
the functionality of the core system. 

They are often used to handle common tasks like pagination, and other utility functions that can be reused across multiple plugins.

Because they are plugins themselves, they have to be defined in the YAML config file, just like any other plugin.
However, the difference is that they have to be defined under the specific plugin that they are extending.


.. _core_pagination_handlers:

Pagination Handlers
------------------------------------
When working with different APIs, you may encounter scenarios where the data is paginated. While pagination is common,
the challenge arises where each API uses a different pagination mechanism. This can make it difficult to write a generic
plugin that can handle pagination for all APIs.

Pagination Handlers solve this problem by providing a modular way to handle pagination. Instead of embedding pagination 
logic within the extraction plugin, you can delegate it to a dedicated pagination handler plugin. 

Below is an example of how to define a utility plugin within the extract phase of the pipeline. In this case, ``SOME_EXTRACT_PLUGIN`` has two arguments:

- `some_arg`: A regular argument passed to the plugin.
- `pagination`: A plugin-in-plugin that requires a plugin and args to be defined.


.. code-block:: yaml

    pipeline:
      ... # Other pipeline configuration
      phases:
        extract:
          plugin: SOME_EXTRACT_PLUGIN
          args:
            some_arg: some_value
            pagination: 
              plugin_id: ID to uniquely identify the pagination plugin
              plugin: SUB_PLUGIN_YOU_WANT_TO_USE
              args:
                some_arg: some_value

Page Based Pagination
^^^^^^^^^^^^^^^^^^^^^
Page based pagination is the most common type of pagination where the API returns a fixed number of records per page.
It looks for a ``has_more`` key in the response and if it is present, it fetches the next page by looking for a ``next_page`` key.

The name of this plugin is ``page_based_pagination``.

**Arguments:**  
There are no arguments required for the page based pagination handler.


HATEOAS Based Pagination
^^^^^^^^^^^^^^^^^^^^^^^^^^
HATEOAS (Hypermedia as the Engina of Application State) is a RESTful API design principle that allows the API to provide
links to related resources in the response.

The name of this plugin is ``hateoas_pagination``.


This plugin leverages these hypermedia links to handle pagination automatically. It looks for a ``links`` or ``_links`` key in the response.
If found, it returns the URL for the next page; otherwise, it returns None, indiciating that there are no more pages to fetch.


**Arguments:**  
There are no arguments required for the page based pagination handler.

|br|

Secret Manager
------------------------------------

.. _core_secret_plugins:

If you haven't already, we recommend reading about :ref:`secrets <secrets>` and how to use them in your pipeline configuration.

Secret Managers plugins provide a secure way to fetch sensitive information like API keys and password from a secret
management service system.

In addition to that, they provide following properties:

- **Lazy Loading:** Secrets are retrieved only when needed during pipeline execution.
- **No sensitive data stored**: Secrets are not stored in the YAML file.
- **Callback mechanism**: Secrets are fetched via a callback function that triggers just before the secret is used.



AWS Secrets Manager Plugin
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``aws_secrets_manager`` plugin is used to fetch secrets from AWS Secrets Manager. 

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
   * - `region`
     - str
     - AWS region where the secret is stored.
     - Required 


**Example Configuration:**  

Secret Managers need to be defined under the ``secrets`` section in the YAML file, and in a separate YAML document, separated by `---`, as 
it is talked about in this :ref:`guide <secrets>`. This guide also explains how to use secrets in your pipeline configuration.


.. code-block:: yaml

    secrets:
      plugin: aws_secret_manager
      args:
        secret_name: SUPER_SECRET_API_KEY
        region: us-west-2

    --- # Separate YAML document

    pipelines:
      # Your pipeline configuration goes here
      ...


.. |br| raw:: html

      <br>