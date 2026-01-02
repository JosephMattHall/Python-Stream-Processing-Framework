# Python Stream Processing Framework (PSPF)

PSPF is a lightweight, extensible stream processing framework for Python 3.11+.

## Features

- **Fluent Pipeline API**: Build data processing pipelines with `map`, `filter`, `reduce`, `window`, and more.
- **Async Runtime**: Built on `asyncio` for efficient IO-bound processing.
- **Pluggable IO**: Easily connect to Kafka, MQTT, Files, or custom sources/sinks.
- **State Management**: Simple per-key state abstraction.

## Installation

```bash
pip install .
```

## Quick Start

See `pspf/examples/inventory_pipeline.py` for a working example.

```python
from pspf.operators.core import Pipeline
from pspf.connectors.file import FileSource, ConsoleSink

# Define a simple pipeline
p = Pipeline()
(p.read_from(FileSource("input.txt"))
  .map(lambda x: x.upper())
  .write_to(ConsoleSink()))

# Run it
p.run()
```

## Project Structure

```text
pspf/
├── runtime/      # Execution engine & checkpoints
├── operators/    # Stream transformations & state
├── connectors/   # IO adapters (Kafka, File, MQTT)
├── cli/          # Command-line interface
└── utils/        # Shared utilities
```

## Contributing

See [docs/contributing.md](docs/contributing.md) for guidelines.
