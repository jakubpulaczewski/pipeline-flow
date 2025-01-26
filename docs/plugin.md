```mermaid
classDiagram





    class PluginLoader {
    }

    class YamlCoordinator {
    }

    class PluginParser {
    }

    PluginLoader o-- YamlCoordinator
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