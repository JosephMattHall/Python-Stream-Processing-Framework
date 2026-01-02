# PSPF Architecture

PSPF (Python Stream Processing Framework) is a Python‑native, async stream
processing framework for building stateful, fault‑tolerant, event‑driven systems.

This document provides a high‑level overview of the core runtime, execution
model, operators, and state management approach used by PSPF.

---

## 🎯 Design Goals

PSPF is designed to be:

- **Python‑first** — simple to read, reason about, and extend
- **Composable** — pipelines are built from small, predictable operators
- **Stateful** — supports per‑key and operator‑scoped state
- **Recoverable** — checkpoint‑based restart without data loss
- **Pluggable** — connectors, stores, and runners are extensible

The framework favors **clarity and determinism** over “black‑box magic.”

---

## 🧩 High‑Level Architecture


┌──────────┐     ┌──────────┐     ┌───────────────┐     ┌──────────┐
│ Sources   │ →→ │ Pipeline │ →→ │ State / Store  │ →→ │ Sinks     │
│ (Kafka,   │     │ Operators│     │ (per‑key, op) │     │ (Kafka,  │
│ MQTT, etc)│     │ map/agg  │     │ + checkpoints │     │ DB, HTTP)│
└──────────┘     └──────────┘     └───────────────┘     └──────────┘
                         │
                         ↓
                 ┌────────────────┐
                 │ Runtime Runner │
                 │ local / dist   │
                 └────────────────┘
A PSPF application consists of:

1: Sources — where events come from

2: Operators — transformations applied to events

3: State Stores — durable or in‑memory state

4: Sinks — where results are emitted

5: Runtime Runner — executes pipelines and manages scheduling

🧱 Core Components
✔️ Sources
Sources produce events and feed them into the pipeline.

Examples:

Kafka topics

MQTT streams

File / log tailing

HTTP event ingestion

Custom user‑defined sources

All sources implement a common asynchronous source interface.

✔️ Operators
Operators are the building blocks of a pipeline.

Common classes of operators:

map — transform each event

filter — conditionally pass events

key_by — partition streams by key

reduce — aggregate values

window — time or count‑based batching

join — combine related streams

custom — user‑defined logic blocks

Operators form a directed pipeline graph.

Each operator may optionally hold:

operator‑scoped state

per‑key state

timers / window clocks

✔️ State & Storage
State is stored via pluggable backends:

In‑memory (development / testing)

Local persistent disk

External store (future)

Two types of state:

Type	Description
Per‑key state	Scoped to a data key (item_id, user_id, etc.)
Operator state	Shared across all events processed by operator

State updates are coordinated with checkpoints for recovery.

✔️ Checkpointing & Recovery
The runtime periodically:

Pauses event advancement

Flushes operator + key state

Writes a durable snapshot

Resumes processing

On restart, PSPF:

reloads last checkpoint

resumes from last processed offset

prevents duplicate processing (best‑effort at‑least‑once initially)

Exactly‑once semantics are a roadmap goal.

⚙️ Execution Model
PSPF uses a cooperative, async execution model built on asyncio.

Key properties:

Operators run as async coroutines

Pipelines advance via cooperative scheduling

Backpressure propagates upstream

Runners control concurrency & throughput

Execution Flow

event → source → operator → operator → sink
Each stage yields control instead of blocking threads.

This enables:

predictable execution

testable pipelines

portable behavior across runners

🏃 Runtime Runners
PSPF separates pipeline definition from execution strategy.

Local Runner (current)
deterministic

single‑process

ideal for development and simulation

Distributed Runner (planned)
shard partitioned streams

worker orchestration

remote state backends

work rebalancing

Runners share the same API — pipelines do not change.

🔌 Connectors
Connectors integrate PSPF with external systems.

Categories:

Sources

Sinks

State stores

Checkpoint writers

Connectors are intentionally thin and composable.

Example implementations (initial):

Kafka

file streams

stdout sink

in‑memory store

🧪 Testing & Determinism
Deterministic execution is a core principle.

This enables:

reproducible local runs

simulation of event sequences

operator‑level unit tests

predictable failure recovery behavior

A test runner provides:

virtual clocks

synthetic event streams

deterministic replay

🛠️ Design Principles
PSPF follows these architectural principles:

Prefer predictable correctness over raw throughput

Expose internals when useful — avoid “magic”

Make operator behavior explicit and observable

Treat state as a first‑class concept

Support gradual evolution toward distributed execution

🗺️ Future Architecture Extensions
Planned enhancements include:

Distributed runner

Remote pluggable state backends

Metrics & topology inspection UI

Exactly‑once processing mode

WASM / sandboxed operator execution (research)

📎 Appendix: Terminology
Term	Meaning
Stream	Continuous sequence of events
Operator	Transformation stage
State Store	Durable storage for operator/key state
Checkpoint	Persistent snapshot for recovery
Runner	Component that executes a pipeline
