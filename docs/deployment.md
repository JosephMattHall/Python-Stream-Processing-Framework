# PSPF Deployment Guide

This guide outlines how to deploy the Python Stream Processing Framework (PSPF) and its applications in production-like environments using Docker.

## 1. Infrastructure Baseline

PSPF requires **Valkey** (for state, offsets, and coordination) and optionally **Prometheus** (for metrics). 

To start the infrastructure baseline:
```bash
docker compose up -d valkey prometheus
```
*   **Valkey**: Listens on `6379`.
*   **Prometheus**: Listens on `9090`. Configure scraping in `prometheus.yml`.

## 2. Containerizing Your Application

Every PSPF application (like the Inventory or Courier examples) should include a `Dockerfile`. A generic one is provided in the project root.

### Building an image:
```bash
docker build -t pspf-app:latest .
```

### Environment Variables:
*   `PSPF_DATA_DIR`: Directory for the local commit log (default: `/data`). Should be a persistent volume.
*   `PYTHONUNBUFFERED`: Ensures logs are emitted immediately.

## 3. Orchestration with Docker Compose

For a complete system (Infra + Multiple Pipes), use the multi-file compose strategy:

```bash
# Start everything: Inventory, Courier, Fraud + Valkey + Prometheus
docker compose -f docker-compose.yml -f docker-compose.apps.yml up -d
```

## 4. Scaling Strategies

### Partition-Based Scaling
PSPF uses **Partition Leases** via Valkey. To scale a pipeline:
1.  Ensure `NUM_PARTITIONS` is high enough (e.g., 16).
2.  Run multiple instances of your application container.
3.  The `PartitionLeaseManager` will ensure each worker only processes its assigned partitions.

## 5. Using PSPF as a Library

To use PSPF in your own projects outside of this repository:

1.  **Install**:
    ```bash
    pip install git+https://github.com/JosephMattHall/Python-Stream-Processing-Framework.git
    ```
2.  **Import**:
    ```python
    from pspf import Pipeline, StreamRecord, Operator
    
    # Build your pipeline
    pipeline = Pipeline().read_from(my_source).map(my_func).write_to(my_sink)
    pipeline.run()
    ```
