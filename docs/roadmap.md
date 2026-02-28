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

## Phase 1 — v0.1.0-alpha
**Status**: Completed

## Phase 2 — v0.2.x-beta
**Status**: Completed

## Phase 3 — v0.5.x-ha
**Status**: Completed

## Phase 4 — v1.0 Production Release
**Status**: Completed (Current)

**Key Accomplishments:**
- Stable Plugin ecosystem.
- Decorator-based stream handlers (`@stream.subscribe`).
- Mature Helm charts and Kubernetes deployment examples.
- Comprehensive crash testing benchmarks.
- Support for complex topologies and joins.
- Transactional state and atomic checkpointing (EOS).
- Cluster rebalancing and failover.
- Interactive queries API.


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
