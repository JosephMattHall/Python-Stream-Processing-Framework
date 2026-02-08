# Observability & Administration

PSPF comes with "batteries-included" observability and management capabilities, allowing you to monitor your stream processors in production with ease.

## Monitoring Metrics

Every PSPF worker automatically exposes a **Prometheus** metrics endpoint on port `8000`.

### Key Metrics

| Metric Name | Type | Description |
|---|---|---|
| `stream_messages_processed_total` | Counter | Total number of messages processed, labeled by `stream` and `status` (success/error/skipped). |
| `stream_processing_seconds` | Histogram | Latency distribution of message processing time. |
| `stream_lag` | Gauge | **Consumer Lag**: The number of pending messages waiting to be processed in the consumer group. |
| `pspf_worker_status` | Gauge | **Worker Status**: `1` if the worker is running, `0` if it is stopped or paused. |

### Accessing Metrics
You can scrape these metrics using any Prometheus-compatible scraper at:
`http://<worker-ip>:8000/metrics`

## Admin API

PSPF includes a lightweight HTTP Admin API for managing worker state. By default, this runs on port `8001`.

### Endpoints

*   **GET /health**: Returns the health status of the worker.
    *   Response: `{"status": "ok", "worker_state": "running"}`
*   **POST /control/pause**: Pauses message consumption. The worker will stop fetching new messages but keep the process alive.
    *   Response: `{"status": "paused"}`
*   **POST /control/resume**: Resumes message consumption.
    *   Response: `{"status": "resumed"}`

### Configuration
You can configure the ports using environment variables in your `settings.py` or `.env` file:

```bash
PROMETHEUS_PORT=8000
ADMIN_PORT=8001
```

## Grafana Dashboard

A pre-built Grafana dashboard is available in `grafana/dashboard.json`. It visualizing:
*   Real-time Throughput using `stream_messages_processed_total`.
*   Consumer Group Lag using `stream_lag` (Critical for auto-scaling).
*   P95 Latency.
*   Worker Availability.

## Reference Stack (Docker Compose)

The repository includes a `docker-compose.monitoring.yml` file that spins up a complete observability stack for testing and reference.

It includes:
1.  **Valkey**: The stream storage backend.
2.  **PSPF Demo Worker**: An example application producing and consuming data.
3.  **Prometheus**: Pre-configured to scrape the demo worker.
4.  **Grafana**: Pre-configured with the PSPF Dashboard.

To run it:
```bash
docker compose -f docker-compose.monitoring.yml up --build
```
Access Grafana at [http://localhost:3000](http://localhost:3000).
