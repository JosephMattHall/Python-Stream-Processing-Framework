# Production Operations

## Logging
PSPF supports structured JSON logging for production environments. Configure this via the `LOG_FORMAT` environment variable.

*   **Development**: `LOG_FORMAT=text` (default) - Human readable logs.
*   **Production**: `LOG_FORMAT=json` - NDJSON format with timestamp, level, logger, message, and context.

## Alerting Examples (Prometheus)

Here are recommended Prometheus alert rules for monitoring PSPF applications.

```yaml
groups:
- name: pspf-alerts
  rules:
  
  # 1. High Consumer Lag
  # Critical: If lag > 1000 for more than 5 minutes
  - alert: HighConsumerLag
    expr: stream_lag > 1000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High lag on {{ $labels.stream }} (Group: {{ $labels.group }})"
      description: "Consumer group is falling behind by {{ $value }} messages."

  # 2. Worker Down/Paused
  # Critical: If worker claims to be running but status is 0
  - alert: WorkerStopped
    expr: pspf_worker_status == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Worker instance is stopped or paused"
  
  # 3. High Error Rate
  # Warning: If > 5% of processed messages range in errors
  - alert: HighErrorRate
    expr: rate(stream_messages_processed_total{status="error"}[5m]) / rate(stream_messages_processed_total[5m]) > 0.05
    for: 2m
    labels:
      severity: warning
```

## High Availability & Clustering

PSPF supports distributed coordination via a `ClusterCoordinator` (backed by Valkey). 

### Node Registration
Each worker registers itself in Valkey with a TTL. You can view the cluster status via the Admin API:
```bash
curl http://localhost:8000/cluster/status
```

### Partition Rebalancing
PSPF implements **Automatic Rebalancing** for zero-downtime scaling:
1. **Fair Share Calculation**: Nodes periodically scan the cluster state to determine the total number of partitions and active nodes.
2. **Voluntary Relinquishment**: if a node holds significantly more partitions than its fair share (e.g., after many new workers join), it will voluntarily release some of its partition locks.
3. **Seamless Handover**: Idle workers will then claim these released partitions and start processing, ensuring an even load distribution.

### Failover
If a worker crashes, its heartbeat TTL in Valkey will expire (default 10s). Other workers will then use `try_acquire_leadership()` to take over the orphaned partitions, ensuring continuous processing.

