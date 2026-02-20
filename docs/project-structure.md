# Project Structure

The PSPF repository is organized into the following core modules:

```text
pspf/
├── admin.py          # Admin API for worker control and status
├── cli.py            # CLI Implementation (pspfctl)
├── cluster/          # High-Availability and Coordination (Raft-like)
├── connectors/       # Backend adapters (Valkey, Kafka, Memory, File)
├── context.py        # Request/Message context
├── log/              # Native binary log (LocalLog, ReplicatedLog)
├── models.py         # core data models (StreamRecord)
├── processing/       # Windowing and advanced logic
├── processor.py      # Core BatchProcessor loop
├── schema.py         # Pydantic schema registry
├── settings.py       # Configuration management (Pydantic Settings)
├── state/            # State Store backends (SQLite, etc.)
├── stream.py         # Main Stream facade (Entry point)
└── telemetry.py      # Metrics (Prometheus) and Tracing (OpenTelemetry)

docs/                 # Documentation (MkDocs)
examples/             # Working example scripts
tests/                # Unit and Integration tests
```

## Key Files
- `pspf/stream.py`: The high-level facade you use in your application.
- `pspf/processor.py`: The reliable processing engine.
- `pspf/connectors/valkey.py`: The primary backend implementation.
