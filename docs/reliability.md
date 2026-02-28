# Reliability & Exactly-Once Semantics

PSPF is designed for enterprise-grade reliability, ensuring that events are processed accurately and recovered automatically in the event of failures.

PSPF achieves **Exactly-Once Semantics (EOS)** through a combination of:
1. **Transactional State Updates**: Built-in state stores (SQLite, RocksDB) support atomic transactions.
2. **Atomic Checkpointing**: The stream offset (checkpoint) is committed in the *same* database transaction as the user's state changes. This ensures that state and offsets never drift.
3. **At-Least-Once Delivery**: Messages are kept in the backend until they are explicitly acknowledged (ACKed). If a crash occurs, the message is redelivered, but the transactional state ensures we don't double-process the effects.

### Atomic Commit Protocol
When a message is processed:
1. A transaction is started in the `StateStore`.
2. Your `handler` is executed, making calls to `ctx.state.put()`.
3. The `BatchProcessor` writes the new stream offset to the internal `__checkpoints__` table.
4. The transaction is **COMMITTED**.
5. Only then is the message ACKed in the backend (Valkey/Kafka).

If the worker crashes between Step 4 and 5, the message will be redelivered. However, since the checkpoint was already committed in Step 3, the `BatchProcessor` will see that this offset has already been processed and skip it (or the state will already match), effectively guaranteeing EOS.

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
