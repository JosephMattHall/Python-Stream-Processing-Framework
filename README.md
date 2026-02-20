# Python Stream Processing Framework (PSPF)

PSPF is a lightweight, high-performance stream processing framework for Python. It provides **Kafka-like semantics** (partitions, offsets, consumer groups, exactly-once processing) **without requiring Kafka** or JVM infrastructure.

It is designed for building event-driven applications, event sourcing systems, and data pipelines that need to be robust, replayable, and easy to deploy.

## Key Features

- **Valkey-Based Streams:** Production-ready stream processing using Valkey (or Redis) as the message broker.
- **Kafka-like Semantics:** Partitions, consumer groups, and high-performance throughput.
- **Exactly-Once Semantics:** Idempotent processing with built-in deduplication.
- **Partitioned Concurrency:** Automatic load balancing across consumer instances.
- **Built-in Observability:** Prometheus metrics, Grafana dashboards, and Admin API.
- **Native Event Log (Preview):** Lightweight file-based log for local-only use cases.

## Installation

```bash
pip install .
```

## Quick Start: User Signups

PSPF makes it easy to handle high-volume event streams. Here is a simple processor that handles user signups.

### 1. Requirements
Ensure you have Valkey (or Redis) running:
```bash
docker run -d -p 6379:6379 valkey/valkey:latest
```

### 2. Run the Demo
The included `valkey_demo.py` example demonstrates a producer and consumer running together.

```bash
python examples/valkey_demo.py
```

### 3. Verification
You will see logs showing events being produced and then consumed with their respective offsets.

## How It Works

1.  **Producers** emit events to a **Stream** using a specific **Backend** (e.g., `ValkeyStreamBackend`).
2.  **Workers** consume from the stream in **Consumer Groups**, sharing the load across multiple instances.
3.  **Processors** handle message batches, providing built-in retries, deduplication, and dead-letter routing.
4.  **Observability** is baked in; every processed message updates metrics and traces.

## Project Structure

```text
pspf/
├── connectors/   # Backend implementations (Valkey, Kafka, Memory)
├── processing/   # Core logic (BatchProcessor, DLO, Retries)
├── state/        # Stateful processing backends (SQLite, etc.)
└── stream.py     # Main Stream facade
```

Check out the [Tutorial](docs/tutorial.md) for a deeper dive.

## Documentation

- [Architecture Guide](docs/architecture.md)
- [Observability Guide](docs/observability.md)
- [Roadmap](docs/roadmap.md)
- [Contributing](docs/contributing.md)
