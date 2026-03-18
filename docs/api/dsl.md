# Functional DSL Reference

The `StreamBuilder` provides a fluent, functional API for defining stream processing pipelines without manually defining handlers for every step.

## Example Usage

```python
from pspf.processing.dsl import StreamBuilder

builder = StreamBuilder(stream)

builder.map(lambda x: {**x, "processed": True}) \
       .filter(lambda x: x["value"] > 100) \
       .sink(output_stream)
```

## API Methods

### `map(func: Callable[[Any], Any])`
Transforms each record in the stream. The function should return the transformed record.

### `filter(func: Callable[[Any], bool])`
Filters records based on a predicate. Records that return `False` are dropped from the pipeline.

### `sink(target_stream: Stream)`
Registers the pipeline to execute and emit the final results to the `target_stream`.

### `run_forever()`
A convenience method that calls `run_forever()` on the underlying `Stream` object.
