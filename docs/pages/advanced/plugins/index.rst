.. _plugins:

Plugins
=======
Plugins extend the functionality of ``pipeline-flow``, allowing users to integrate with 
different data sources, apply custom transformations, and enhance pipeline capabilities without 
modifying the core system.

If you haven't already, check out the :ref:`Plugin Core Concepts <plugin_core_concepts>` to understand the basics of plugins.

Plugin categories
-------------------
There are three types of categories in ``pipeline-flow``:

#. **Built-in Plugins**  

   - Official plugins provided by the ``pipeline-flow`` core library.  
   - Fully supported and maintained by the core development team.  
   - Guaranteed to be compatible with the latest versions of ``pipeline-flow``.  

#. **Community Plugins**  

   - Developed and maintained by the ``pipeline-flow`` community.  
   - Useful for specialized use cases not covered by built-in plugins.  
   - May vary in terms of support and update frequency.  

#. **Custom Plugins**  

   - Developed by users to fit their unique requirements.  
   - Provides flexibility for project-specific logic or third-party integrations.  
   - Easily integrated by following the :ref:`guide <plugin_development>`.  



.. _plugin_hierarchy:

Plugin Hierachy
-------------------
Each plugin belongs to a **plugin category** (Built-in, Community or Custom) and can
be further classified into one of the following **plugin types**:

#. **Phase Plugins** 

   - Define the **core phases** of a pipeline, such as Extract, Transform, and Load.  

#. **In-Phase Plugins**  

   - Operate **within phases** to enhance or modify data during processing.  
   - Examples: iMergeExtractPlugin enables to merge all extracted data from multiple sources into a single data structure.

#. **Utility Plugins**  

   - Provide **general-purpose functionality** across the pipeline but are not tied to any specific phase.  
   - Examples: Secret Managers, Pagination Handlers, etc.

.. figure:: ../../../_static/plugins_hierarchy.png
   :align: center
   :width: 80% 
   :alt: Plugin Hierachy


Next Steps
-------------
- Explore :ref:`Core Plugins <core_plugins>` to learn about the built-in plugins available out of the box.
- Discover :ref:`Community Plugins <custom_plugins>` to find plugins contributed by the community.
- Explore the Plugin Development Guide to build your own :ref:`custom plugins <plugin_development>`.
- Contribute to the Community Plugin Repository and help others build better pipelines.

Reference
-------------

.. toctree::
   :maxdepth: 2
   :caption: Reference

   core/core_plugins
   community_plugins
   custom_plugins
   plugin_development
   sync_to_async_plugins.rst
