# Python Stream Processing Framework (PSPF)

PSPF is a lightweight, high-performance stream processing framework for Python. It provides **Kafka-like semantics** (partitions, offsets, consumer groups, exactly-once processing) **without requiring Kafka** or JVM infrastructure.

It is designed for building event-driven applications, event sourcing systems, and data pipelines that need to be robust, replayable, and easy to deploy.

> **Note**: While the Quick Start below uses Valkey (Redis), PSPF is backend-agnostic! You can also use Kafka, or the local-only Memory and File backends for testing without Docker.

- **Auto-Instantiation:** Simply provide a topic and group; PSPF handles backend setup automatically (Valkey with Memory fallback).
- **Decorator API:** Simple `@stream.subscribe` and `@stream.window` handlers for rapid development.
- **Durable Retries:** Message retry state is persisted in a `StateStore`, surviving worker restarts.
- **Idempotent Sinks:** Built-in `BaseSink` for external side-effects (APIs, DBs) with automatic idempotency tokens.
- **Exactly-Once Semantics:** Atomic transactions where state and offsets are committed together.
- **Reliability & DLQ:** Built-in retries and Dead Letter Queues (DLQ) for failed or late events.
- **Zero-Downtime Scaling:** Automatic partition rebalancing across worker clusters.
- **Cloud Native:** Built-in Helm charts for Kubernetes and Prometheus monitoring.
- **Powerful CLI:** Inspect logs, manage consumer groups, and handle DLQs directly.

## Installation

```bash
pip install pspf
```

## Quick Start: User Signups

PSPF makes it easy to handle high-volume event streams. 

```python
from pspf import Stream

# Auto-instantiates Valkey (fallback to Memory if Valkey is unavailable)
stream = Stream(topic="signups", group="group1")

@stream.subscribe("signups")
async def handle_signup(event):
    print(f"Welcome {event['email']}!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(stream.run_forever())
```

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

## About the Author

**PSPF™** was designed and developed by Joseph Hall.

I built this framework to solve specific challenges in stream processing—focusing on low-latency data handling, simplified deployments without JVM overhead, and ease of use. While my current professional background is in high-volume customer success and sales, my passion lies in engineering robust, scalable Python tools.

## 🚀 Let's Connect

I am currently looking for my next challenge in Software Engineering, Data Engineering, or DevOps. If you’re looking for a developer who understands both clean code and how to communicate with stakeholders, let’s talk!

- **GitHub:** [https://github.com/JosephMattHall](https://github.com/JosephMattHall)
- **Email:** joseph@josephmatthewhall.com
