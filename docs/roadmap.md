# PSPF Production Roadmap

**Vision**: Build PSPF into a lightweight, production-safe, embeddable stream processing framework for Python, optimized for small-to-medium systems and edge deployments.

**Target Positioning**: "The SQLite of Stream Processing for Python"

A hybrid of:
- **Path A**: Lightweight Kafka-style system
- **Path B**: Edge / Embedded Streaming Platform

## Guiding Principles

- **Reliability before scale**
- **Simple deployment over complex clustering**
- **Strong local durability**
- **First-class developer experience**
- **Observable and debuggable by default**
- **Suitable for embedded and on-prem use**

---

## Phase 0 — Core Stability & Durability (v0.5 → v0.7)
**Target**: 1–2 Months

**Goals**
- Make single-node PSPF reliable under crashes
- Prevent data corruption
- Ensure safe recovery

**Deliverables**
- [ ] Log segmentation (rotating log files)
- [ ] Per-record checksums (CRC32 or similar)
- [ ] Startup recovery scanner
- [ ] Safe truncation on corruption
- [ ] Atomic offset commits
- [ ] Improved error handling
- [ ] Crash/failure test suite
- [ ] Repository cleanup

**Success Criteria**
- No data loss after unclean shutdown
- Safe replay after crash
- All corruption scenarios handled

---

## Phase 1 — Operability & Observability (v0.7 → v0.9)
**Target**: 1–2 Months

**Goals**
- Make PSPF easy to operate in production
- Improve debuggability
- Enable basic administration

**Deliverables**
- [ ] Structured logging
- [ ] Health endpoints
- [ ] Admin HTTP API
- [ ] Cluster and partition status endpoints
- [ ] CLI tool: `pspfctl`
- [ ] Built-in diagnostics
- [ ] Prometheus alert examples
- [ ] Grafana dashboard templates

**Success Criteria**
- Operators can diagnose failures quickly
- Basic cluster management from CLI
- Metrics cover all major subsystems

---

## Phase 2 — High Availability & Replication (v0.9 → v1.1)
**Target**: 3–4 Months

**Goals**
- Allow PSPF to survive node failures
- Enable basic multi-node deployments
- Prevent single points of failure

**Deliverables**
- [ ] Leader/follower partition model
- [ ] Synchronous replication
- [ ] Metadata store (Valkey/etcd-based)
- [ ] Leader election
- [ ] Automatic failover
- [ ] Replica resynchronization
- [ ] Replication lag monitoring

**Success Criteria**
- No data loss on node failure
- Automatic leader promotion
- Zero manual recovery steps

---

## Phase 3 — Embedded Stateful Processing (v1.1 → v1.3)
**Target**: 2–3 Months

**Goals**
- Support stateful stream processing
- Enable analytics and aggregations
- Improve processing guarantees

**Deliverables**
- [ ] Pluggable state stores (SQLite/RocksDB/LMDB)
- [ ] Local state directories
- [ ] Periodic snapshots
- [ ] Snapshot restore logic
- [ ] Windowed aggregations
- [ ] Session tracking
- [ ] Improved exactly-once semantics

**Success Criteria**
- Stateful apps recover correctly
- No state loss after crash
- Consistent replay behavior

---

## Phase 4 — Horizontal Scaling & Rebalancing (v1.3 → v1.5)
**Target**: 3–4 Months

**Goals**
- Enable elastic scaling
- Support growing workloads
- Improve cluster utilization

**Deliverables**
- [ ] Partition reassignment engine
- [ ] Automatic rebalancing
- [ ] Online partition migration
- [ ] Zero-downtime scaling
- [ ] Network protocol formalization
- [ ] Load-based partition distribution

**Success Criteria**
- Nodes can join/leave safely
- No manual redistribution
- Minimal processing interruption

---

## Phase 5 — Developer Experience & Ecosystem (v1.5+)
**Target**: Ongoing

**Goals**
- Maximize adoption
- Reduce onboarding friction
- Grow ecosystem

**Deliverables**
- [ ] High-level application API
- [ ] Decorator-based stream handlers
- [ ] Starter templates
- [ ] Project generator (`pspf new`)
- [ ] Official connectors library
- [ ] Cloud-native deployment examples
- [ ] Kubernetes and Helm charts

**Success Criteria**
- New users productive in <30 minutes
- Active community contributions
- Multiple production users

---

## Versioning Strategy
- **v0.x**: Experimental / Early adopters
- **v1.0**: Stable single-node + replication
- **v1.x**: Feature expansion
- **v2.0**: Major architectural upgrades

Semantic versioning will be followed after v1.0.

---

## Primary Target Use Cases

### Small & Medium Systems
- Internal analytics pipelines
- Microservice event processing
- SaaS backend workflows
- ETL pipelines

### Edge & Embedded Systems
- IoT gateways
- Factory systems
- Retail devices
- On-prem monitoring
- Offline-first analytics

## Non-Goals
The following are intentionally deprioritized:
- Multi-datacenter replication
- Global geo-replication
- Petabyte-scale clusters
- Fully managed SaaS offering
- JVM-level throughput competition

**PSPF prioritizes simplicity and reliability over extreme scale.**

---

## Long-Term Vision (3+ Years)
- Reference embedded streaming engine for Python
- Default choice for local-first event processing
- Widely used in edge environments
- Stable plugin ecosystem
- Enterprise support options

---

## Community & Contribution Roadmap

**Short Term**
- Improve contributor documentation
- Add development environment setup guide
- Create "good first issue" labels

**Mid Term**
- Contributor review process
- Release management guidelines
- Public roadmap updates

**Long Term**
- Technical steering group
- Maintainer onboarding program
- Governance model

---

## Current Focus (Next 90 Days)
Priority for upcoming development:
- Log segmentation and recovery
- Checksum validation
- Crash testing framework
- Structured logging
- CLI foundation

These items form the foundation for **v1.0 readiness**.

### Milestone: Production-Ready v1.0
v1.0 is considered production-ready when:
- Logs are fully recoverable
- Replication is stable
- CLI tooling is complete
- Monitoring is mature
- Documentation is comprehensive
- At least 3 real-world deployments exist
