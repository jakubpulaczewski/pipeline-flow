```mermaid
classDiagram





    class PluginLoader {
    }

    class YamlCoordinator {
    }

    class PipelineParser {
    }

    class PluginParser {
    }

    PluginLoader o-- YamlCoordinator
    YamlCoordinator *-- PipelineParser 
    YamlCoordinator *-- PluginParser 
```






<|--	Inheritance
*--	Composition
o--	Aggregation
-->	Association
--	Link (Solid)
..>	Dependency
..|>	Realization
..	Link (Dashed)