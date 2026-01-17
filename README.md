# Python Stream Processing Framework (PSPF)

PSPF is a lightweight, high-performance stream processing framework for Python. It provides **Kafka-like semantics** (partitions, offsets, consumer groups, exactly-once processing) **without requiring Kafka** or JVM infrastructure.

It is designed for building event-driven applications, event sourcing systems, and data pipelines that need to be robust, replayable, and easy to deploy.

## Key Features

- **Native Event Log:** Built-in file-based partitioned log. No external message broker required.
- **Kafka-Free Architecture:** Runs entirely in Python. Data is stored in efficient binary **MessagePack** format.
- **Exactly-Once Semantics:** Idempotent processing with deduplication to ensure events are effectively processed once.
- **Partitioned Concurrency:** Automatic partitioning by key ensures strict ordering for entities (e.g., "Item A") while allowing parallel processing across partitions.
- **Event Sourcing Ready:** Perfect for building systems where state is derived from an immutable log of events.

## Installation

```bash
pip install .
```

## Quick Start: Inventory System

PSPF includes a complete **Inventory Management System** example that demonstrates optimistic concurrency, rollback/compensation, and event sourcing.

### 1. Run the API & Worker
The example starts a FastAPI server and a background PSPF worker.

```bash
uvicorn examples.inventory_app.api:app --host 0.0.0.0 --port 8000
```

### 2. Run the Verification Script
This script creates items, checks them in/out, and tests the "No Negative Stock" invariant by attempting an overdraft.

```bash
python examples/inventory_app/verify.py
```

## How It Works

1.  **Producers** (like a Web API) append events to the **Native Log** (`LocalLog`).
    *   Events are sharded into partitions based on their Key (e.g., Item ID).
    *   Data is stored on disk in binary MessagePack format for speed.
2.  **Workers** (`LogSource`) read from the log.
    *   Each partition is processed sequentially to guarantee order.
    *   Consumer groups track **Offsets** to know where they left off.
3.  **Processors** apply events to state.
    *   If a process crashes, it restarts and replays from the last committed offset.
    *   Deduplication ensures crashed events aren't processed twice.

## Project Structure

```text
pspf/
├── log/          # Native Log implementation (MessagePack/File-based)
├── runtime/      # Execution engine (PartitionedExecutor, Dedup)
├── connectors/   # IO adapters (LogSource, etc.)
└── models.py     # StreamRecord definition

examples/
└── inventory_app/ # Full Event Sourcing Application
```

## Documentation

- [Architecture Guide](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [Contributing](docs/contributing.md)
