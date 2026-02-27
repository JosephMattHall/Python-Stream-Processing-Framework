# PSPF Production Roadmap

**Vision**: Build PSPF into a lightweight, production-safe, embeddable stream processing framework for Python, optimized for small-to-medium systems and edge deployments.

**Target Positioning**: "The SQLite of Stream Processing for Python"

A hybrid of:
- **Path A**: Lightweight Kafka-style system via networked backends (Valkey, Kafka).
- **Path B**: Edge / Embedded Streaming Platform via local backends (File, Memory).

## Guiding Principles

- **Reliability before scale**
- **Simple deployment over complex clustering**
- **Backend-agnostic flexibility**
- **First-class developer experience**
- **Observable and debuggable by default**
- **Suitable for embedded and on-prem use**

---

## Phase 1 — v0.1.0-alpha (Current)
**Status**: Alpha Release

**Goals completed:**
- Core `Stream` and `BatchProcessor` facades.
- Multi-backend architecture (Valkey, Kafka, Memory, File/LocalLog).
- Pydantic-based schema validation and registry.
- Basic consumer group semantics for Valkey and Kafka.
- Exactly-once semantics through idempotent processing and dead-letter queues.
- OpenTelemetry and Prometheus observability out of the box.
- CLI foundation (`pspfctl`).

---

## Phase 2 — v0.2.x Beta 
**Focus**: Operability, robustness, and stateful processing.

**Goals:**
- **LocalLog Advancements**: Log segmentation (rotating log files), safe recovery scanning after crash, and per-record checksums.
- **State Store Integration**: Simplify attaching state stores (`SQLite`, `RocksDB`) directly via the high-level `Stream` facade.
- **Admin API Expansion**: Expand the `8001` control endpoints to report detailed partition cluster status.
- **CLI Enhancements**: Add cluster management, diagnostic checks, and more fine-grained replay tools to `pspfctl`.
- **Structured Logging Enhancements**: Further refine NDJSON output context.
- **Time Semantics & Watermarks**: Introduce Event Time vs Processing Time concepts and watermarking for handling late-arriving data.
- **Windowing Primitives**: Basic Tumbling and Sliding window support to enable bounded aggregations on unbounded streams.
- **State Store TTL**: Time-to-live and automatic compaction mechanisms for state stores to prevent memory bloat over time.

---

## Phase 3 — v0.5.x High Availability 
**Focus**: Multi-node durability.

**Goals:**
- **Replication**: Leader/follower partition model for the Native File Log.
- **Failover**: Automatic leader election and resynchronization.
- **Rebalancing**: Zero-downtime scaling and automatic consumer partition distribution for non-networked backends.
- **Transactional State (EOS)**: Atomic processing guarantees across offset commits and state store updates.
- **Interactive Queries**: Expose local RocksDB/Memory state stores via API so external services can query aggregations without a secondary database.

---

## Phase 4 — v1.0 Production Release
**Focus**: Stability and ecosystem growth.

**Goals:**
- Stable Plugin ecosystem.
- Decorator-based stream handlers (`@stream.subscribe`).
- Mature Helm charts and Kubernetes deployment examples.
- Comprehensive crash testing benchmarks.
- **Complex Topologies & Joins**: Support for stream-to-stream and stream-to-table joins.

---

## Versioning Strategy
- **v0.1.x**: Alpha (Breaking changes possible but documented)
- **v0.2.x - v0.9.x**: Beta / Pre-production feature expansion
- **v1.0**: Stable production API

Semantic versioning will strictly be followed after v1.0.

---

## Target Use Cases

### Small & Medium Systems
- Internal analytics pipelines.
- Microservice event processing (replacing Celery/RabbitMQ for event-driven logic).
- SaaS backend workflows.

### Edge & Embedded Systems
- IoT gateways and on-prem local aggregations.
- Offline-first analytics.

## Non-Goals
The following are intentionally deprioritized:
- Global geo-replication.
- Petabyte-scale massive concurrency.
- JVM-level extreme throughput competition.

**PSPF prioritizes simplicity and reliability over extreme scale.**
