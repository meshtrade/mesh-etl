# Mesh ETL

Mesh ETL is a modular, generic golang ETL pipeline builder. This pipeline builder was designed with compile-time correctness in mind, by leveraging go generics we can achieve this (somewhat).

## Semantics & Rationale

A pipeline at a high level is logically organised into three components, a source, a stage and a sink. The mental model here, is that these components each represent the stages of an ETL pipeline respecetively. Now an actual pipeline setup might use composition operators to make each stage more complex and to suit the different use cases.

```go
Pipeline(
    Source( 
        ...
    ),
    Stage(
        ...
    ),
    Sink(
        ...
    )
)
```

### Source

A source component should be responsible for ingesting data from some resilient data store or streams. Sources can either be chained or joined. Chaining sources is useful when you need to combine data from multiple data stores or streams, in a sequence.

#### Source Operators

- ```SequenceSource(Source[T], ChainedSource[T,V]) -> Source[V]```: Used to chain together two depdent sources, the second source is a special source implementation that takes the result returned from the first source and passes it to the second

### Stage

A bit of an awkward name, but a stage is used to take a collection of data, perform some form of processing and then return a result which is another collection of data. 

#### Stage Operators

- ```Map(func(T) -> V) -> Stage[T,V]```: Used to 'map' one type to another, the given map function will be executed for each element in the data collection ([]T) it will map an element of type T to an element type V.
- ```Filter(func(T) -> bool) -> Stage[T,T]```: Used to filter out elements out of a data collection that meet some predicate.
- ```SequenceStage(Stage[T,V], Stage[V,K]) -> Stage[T,K]```: Used to chain together two stages, to chain together more stages you can nest sequences for example: 
```
    Sequence(
        Sequence(
            Stage[...],
            Stage[...]
        ),
        Stage[...]
    )
```

### Sink

A sink is the exit point for the pipeline and is where data is either persisted to another resilient data store or moved to another data stream. Sinks can be composed in a chain or can be 'spread', where multiple sinks are executed in parallel.

#### Sink Operators

- ```SequenceSink(Sink[T]...): -> Sink[T]```: Used to chain together multiple sinks.
- ```Spread(Sink[T]...) -> Sink[T]```: Used to execute multiple sinks in parallel.
