# PSPF Architecture

PSPF (Python Stream Processing Framework) is designed to bring specific "Big Data" guarantees—partitioning, ordering, and replayability—to standard Python applications without the operational overhead of managing a Kafka cluster.

## 🎯 Core Concepts

### 1. The Log (`LocalLog`)
Instead of a remote broker, PSPF uses a **Native Log**.
- **Storage:** Data is written to local disk in `.pspf_data/`.
- **Format:** Events are serialized using **MessagePack** (binary) with length-prefixed framing.
- **Partitioning:** The log is split into $N$ files (`partition_0.bin`, `partition_1.bin`, ...).
- **Hashing:** `partition = hash(key) % num_partitions`. This ensures all events for the same entity (e.g., "User 123") always go to the same file.

### 2. The Record (`StreamRecord`)
The canonical unit of data is the `StreamRecord`:
```python
@dataclass
class StreamRecord:
    id: str                  # Unique Event ID (UUID)
    key: str                 # Partition key
    value: Any               # Event payload (Dict)
    timestamp: datetime      # Event creation time
    partition: int
    offset: int
```

### 3. Execution Model (`PartitionedExecutor`)
PSPF enforces a strict concurrency model to preventing race conditions:
- **Per-Partition Sequential:** A single partition is never processed concurrently. This guarantees that if "Create Item" comes before "Checkout Item" in the log, it will happen in that order.
- **Cross-Partition Parallel:** Different partitions are processed in parallel asyncio tasks.

### 4. Exactly-Once Processing
To ensure correctness even during crashes, PSPF implements **Idempotency**:
1.  **Deduplication Store:** Before processing an event, the system checks if `event.id` has been seen.
2.  **At-Least-Once Delivery:** If a worker crashes, it replays from the last checkpoint. This might re-deliver the last few events.
3.  **Result:** The Deduplication Store blocks the re-delivered events, ensuring the side effect happens exactly once.

*Note: The current implementation uses an In-Memory Deduplication Store. For production durability across full system restarts, this should be swapped for a Redis or SQLite backend.*

## 🧩 Component Diagram

```mermaid
graph TD
    API[FastAPI / Producer] -->|Append(Record)| Log[LocalLog .bin]
    
    subgraph "PSPF Worker"
        Log -->|Read(Offset)| Source[LogSource]
        Source -->|StreamRecord| Dedup{Dedup Check}
        
        Dedup -->|Duplicate| Skip[Skip]
        Dedup -->|New| Proc[Processor]
        
        Proc -->|Update| State[(State Store)]
        Proc -->|Commit| Offsets[OffsetStore]
    end
```

## ⚙️ IO & Extensibility

While PSPF includes a native log, the abstractions (`Log`, `Source`, `Sink`) are decoupled. You can implement:
- **KafkaLog:** Replace `LocalLog` with a wrapper around `aiokafka` to scale to multiple servers.
- **RedisOffsetStore:** Store consumer offsets in Redis for distributed workers.
- **PostgresState:** Store entity state in a relational DB instead of memory.
