# PSPF Roadmap 2026: Hardening for Scale & Security

This roadmap outlines the evolution of PSPF from a functional framework to an enterprise-hardened streaming platform.

---

## Phase 1: Security & Persistence (Hardening)
**Objective**: Eliminate high-risk dependencies and ensure crash-resilience for all connectors.

- **[ ] Serialization Migration**:
    - Replace `pickle` with **Msgpack** as the default binary format in SQLite and RocksDB backends.
    - Implement a "Safe JSON" fallback for human-readable state inspection.
- **[ ] Durable Retry Tracking**:
    - Refactor `KafkaStreamBackend` to store retry counters in the `StateStore` instead of in-memory.
    - Ensure message retry state survives worker restarts.
- **[ ] Idempotent Sinks**:
    - Provide a standard `BaseSink` that enforces idempotency tokens for external side-effects (API calls, DB writes).

## Phase 2: High-Performance Connectivity (Interop)
**Objective**: Enhance Kafka reliability and support enterprise data formats.

- **[ ] Kafka Low-Watermark Commits**:
    - Implement a local offset manager for Kafka that tracks the "low-watermark" of a batch.
    - Ensure that failing messages stop the offset progression of the entire partition to prevent data loss.
- **[ ] Pluggable Serializers**:
    - Support **Protobuf** and **Avro** as first-class schemas.
    - Add a `SchemaRegistry` integration for Kafka.
- **[ ] Batch Performance Tuning**:
    - Introduce `aiokafka` producer batching and compression settings in `ValkeySettings`.

## Phase 3: Operational Excellence (Stability)
**Objective**: Simplify deployment and improve cluster-wide stability.

- **[ ] Resilient Port Binding**:
    - Implement automatic port incrementing for Admin API and Telemetry if the default port is occupied.
    - Expose `actual_port` in the cluster metadata for coordinator discovery.
- **[ ] Optimized Cluster Rebalancing**:
    - Extract rebalancing logic from the 3s heartbeat loop into a membership-triggered `StateSync` event.
    - Reduce Valkey/Redis overhead by 40% in large clusters.
- **[ ] Native CLI Tooling**:
    - Implement `pspf-cli` for live stream tailing, manual DLO reprocessing, and state inspection via the Admin API.

---

## Evaluation Metrics
- **Data Loss Rate**: Zero (Verified via Chaos testing).
- **Startup Time**: < 1s (Delayed Admin start).
- **Security Compliance**: No `pickle` usage in production paths.
