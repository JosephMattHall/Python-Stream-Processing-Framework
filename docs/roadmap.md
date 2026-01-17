# PSPF Roadmap

## ✅ Completed Milestones

### Phase 1: Core Foundations
- [x] **Canonical Data Model**: Defined `StreamRecord`.
- [x] **Native Log**: Implemented partitioned, file-based log (`LocalLog`).
- [x] **Partitioned Execution**: Implemented strictly ordered processing per partition.

### Phase 2: Reliability & Correctness
- [x] **Event Sourcing**: Proven end-to-end with the Inventory App.
- [x] **Compensating Transactions**: Logic to handle business rule violations (e.g. over-stock).
- [x] **Exactly-Once Semantics**: Implemented Deduplication logic to handle replay.

### Phase 3: Performance
- [x] **Binary Storage**: Migrated log format from JSON to **MessagePack** for high throughput.

---

## 🚧 Upcoming Priorities

### Phase 4: Production Hardening (Enterprise Ready)
The current system is robust for single-node deployments. To rival Kafka/Flink in distributed settings, we need:

1.  **Durable Coordination**
    - [ ] **Redis/Postgres Leases**: Allow multiple PSPF Nodes to coordinate which partitions they own (prevent Split Brain).
    - [ ] **Durable Offset Store**: Move offsets from Memory to Redis/SQLite so they survive process restarts.

2.  **Schema Evolution**
    - [ ] **Schema Registry**: Integration with **Avro** to validate events at the door.
    - [ ] **Compaction**: Implement log compaction to discard old updates for keys (snapshotting state).

3.  **Distributed Deployment**
    - [ ] **Cluster Mode**: A coordination protocol to auto-balance partitions across a fleet of workers.
    - [ ] **CloudLog**: An S3/GCS-backed Log implementation for infinite retention.

## 🧭 Principles

1.  **Correctness First**: Never lose data. Never process an event twice (effect-wise).
2.  **Simple Operations**: " pip install" should be all you need to start.
3.  **Pluggable**: Every component (Log, Store, Serializer) can be swapped.
