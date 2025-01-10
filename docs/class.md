```mermaid
classDiagram

    class PluginRegistry {
    }


    class PipelinePhase {
        <<enumeration>>
        EXTRACT_PHASE
        TRANSFORM_PHASE
        LOAD_PHASE
        TRANSFORM_AT_LOAD_PHASE
    }

    class PipelineType {
        <<enumeration>>
        ETL
        ELT
        ETLT
    }



    class ExtractPhase {
    }

    class TransformPhase {
    }

    class LoadPhase {
    }


    class TransformLoadPhase {
    }


    class Pipeline {
    }

    class BasePhase {
        <<interface>>
    }


    class PipelineOrchestrator {
    }

    class PipelineStrategy {
    }


    class ETLStrategy {
    }
    class ELTStrategy {
    }

    class ETLTStrategy {
    }



    BasePhase <|-- ExtractPhase
    BasePhase <|-- TransformPhase
    BasePhase <|-- LoadPhase
    BasePhase <|-- TransformLoadPhase

    Pipeline --> PipelinePhase
    Pipeline --> PipelineType
    Pipeline --> ExtractPhase
    Pipeline --> TransformPhase
    Pipeline --> LoadPhase
    Pipeline --> TransformLoadPhase

    PipelineOrchestrator --> Pipeline
    ETLStrategy ..|> PipelineStrategy
    ELTStrategy ..|> PipelineStrategy
    ETLTStrategy ..|> PipelineStrategy

    PluginRegistry --> PipelinePhase
```






<|--	Inheritance
*--	Composition
o--	Aggregation
-->	Association
--	Link (Solid)
..>	Dependency
..|>	Realization
..	Link (Dashed)
