# Reliability & Exactly-Once Semantics

PSPF is designed for enterprise-grade reliability, ensuring that events are processed accurately and recovered automatically in the event of failures.

## Exactly-Once Semantics

PSPF achieves **Exactly-Once Semantics (EOS)** through a combination of:
1. **At-Least-Once Delivery**: Messages are kept in the backend until they are explicitly acknowledged (ACKed) by the worker.
2. **Idempotent Processing**: Built-in deduplication ensures that if a message is redelivered (e.g., after a worker crash), it is effectively processed only once.

### Deduplication
When using the `Stream` facade with a supported backend, PSPF tracks message IDs and ensures that the user-provided `handler` is not invoked twice for the same message ID within a configurable window.

## Failure Handling

### Retries
If the `handler` raises an exception, the `BatchProcessor` catches it and manages retries:
1. **Immediate Retry**: The message remains in the Pending Entries List (PEL).
2. **Retry Count**: Each message tracks its retry count.
3. **Dead Letter Queue (DLQ)**: If a message exceeds `max_retries` (default: 3), it is moved to a DLQ and acknowledged in the main stream to prevent blocking other messages.

### Dead Letter Office (DLO)
Messages in the DLQ can be inspected and re-driven manually using the Admin API or custom scripts. They typically contain:
- The original payload.
- The error message that caused the failure.
- The timestamp of the final failure.

## Worker Recovery (`XAUTOCLAIM`)

When a worker instance crashes, it leaves "in-flight" messages assigned to it. PSPF implements automatic worker recovery:
- **Idle Timeout**: If a message has been in a worker's PEL for longer than `min_idle_time_ms` (default: 60s), it is considered "stuck".
- **Claiming**: Live workers periodically scan for stuck messages using `XAUTOCLAIM` (in Valkey) and take ownership of them.
- **Resumption**: The new worker processes the claimed message and ACKs it, ensuring no data loss.
