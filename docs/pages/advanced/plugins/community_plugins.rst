.. _community_plugins:

Community Plugins
========================
The community can contribute plugins to the ``pipeline-flow`` ecosystem. Community plugins can be shared with others 
and used in different pipelines. To use a community plugin, you need to install the plugin package using a package manager like
pip or poetry.

It is worth noting that community plugins are not part of the core system and need to be imported explicitly, 
as well defined in the yaml configuration file.

.. code:: bash

  pip install 'pipeline-flow-community[PLUGIN_NAME]'
  
where ``PLUGIN_NAME`` is the name of the plugin defined as an extra dependency in the plugin package.


For a list of available plugins, visit the `Community Plugin Repository <https://github.com/jakubpulaczewski/pipeline-flow-community>`_.


Next Steps
-------------
- If you are interested in contributing to the community plugins, check out the :ref:`Plugin Development Guide <plugin_development>` to get started.