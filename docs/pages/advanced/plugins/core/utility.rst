.. _core_misc_plugins:

Utility Plugins
========================
``pipeline-flow`` provides a set of utility plugins that are designed to further enhance and extend the functionality
of the core system.



Pagination Handlers
------------------------------------

.. _core_pagination_handlers:

When working with different APIs, you may encounter scenarios where the data is paginated. There are different types
of pagination mechanism used by APIs. 

Pagitional Handlers are designed to handle these scenarios by enabling the same plugin to fetch data from 
different paginated APIs.

.. warning::
   Currently, paginator handlers are hard coded and cannot be extended. We are working on making them to be configurable
   and extendable in the future releases.



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
