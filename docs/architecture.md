# Architecture

The **Python Stream Processing Framework (PSPF)** uses a **Composition-based** architecture to provide flexibility, testability, and enterprise-grade reliability.

## Core Components

The system is built around three main components that are injected into the high-level `Stream` facade.

```mermaid
graph TD
    UserCode[User Application] --> Stream[Stream Facade]
    Stream --> Backend[StreamingBackend]
    Stream --> Processor[BatchProcessor]
    Stream --> Schema[Pydantic Schema]
    
    Backend --> Connector[Connector]
    Connector --> DB[(Valkey/Kafka/File)]
```

### 1. Stream Facade
The `Stream` class acts as the entry point. It coordinates the other components but does not contain low-level logic itself. It handles:
- **Dependency Injection**: Takes a configured Backend and Schema.
- **Context Management**: `async with` support for resource cleanup.
- **Tracing**: Automatically injects OpenTelemetry contexts.

### 2. StreamingBackend
Handles all interactions with the underlying storage layer (Valkey, Kafka, Memory, or File).
- **Connector**: Manages the connection pool or file handles.
- **Stream Operations**: Producing, consuming, and acknowledging events.
- **Reliability**: Implements Worker Recovery (e.g., `XAUTOCLAIM` for Valkey) and Dead Letter logic mapping.

### 3. BatchProcessor
The engine that drives the consumption loop.
- **Batching**: Reads messages in chunks for efficiency.
- **Signal Handling**: Gracefully shuts down on `SIGTERM`.
- **Observability**: Updates Prometheus metrics and starts Tracing spans.

### 4. Schema (Pydantic)
Ensures data integrity.
- **Validation**: All incoming/outgoing data is validated against a Pydantic model.
- **Serialization**: Automatic JSON serialization.

## Data Flow

1. **Emit**: User calls `stream.emit(event)`. Data is validated, tracing context is injected, and it's written to the Backend.
2. **Consume**: `BatchProcessor` polls the Backend for a batch of messages.
3. **Process**: Each message is deserialized into a Pydantic object and passed to the user's `handler`.
4. **ACK**: If successful, the message is ACKed.
5. **Failure**: If processing fails, it is retried. If retries exceed the limit, it is moved to a Dead Letter Queue (DLQ).
