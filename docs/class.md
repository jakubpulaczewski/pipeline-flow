```mermaid
classDiagram

    class PluginFactory {
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


    class IExtractor {
        <<interface>>
        +extract_data()
    }

    class ILoader {
        <<interface>>
        +load_data(data)
    }


    class ITransform {
        <<interface>>
        +transform_data(data)
    }

    class ILoadTransform {
        <<interface>>
        +transform_data()
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
    PipelineOrchestrator ..> PipelineStrategyFactory
    PipelineStrategyFactory  --> PipelineType
    ETLStrategy ..|> PipelineStrategy
    ELTStrategy ..|> PipelineStrategy
    ETLTStrategy ..|> PipelineStrategy

    PluginFactory --> PipelinePhase
```






<|--	Inheritance
*--	Composition
o--	Aggregation
-->	Association
--	Link (Solid)
..>	Dependency
..|>	Realization
..	Link (Dashed)