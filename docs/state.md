# Stateful Processing

PSPF supports **Stateful Stream Processing**, allowing your handlers to maintain and update persistent state (e.g., counters, running totals, or session data).

## State Stores

State is managed via a `StateStore` interface. Currently supported backends:
- **SQLite**: Local, file-based state (perfect for small-to-medium datasets).
- **RocksDB**: High-performance, embedded Key-Value store (recommended for high-throughput stateful processing).
- **In-Memory**: Fast, but state is lost on restart.

## Using State in Handlers

To use state, your handler function must accept a `Context` object as its third argument.

```python
from pspf import Context

async def process_word(msg_id, data, ctx: Context):
    word = data.get("word")
    
    # 1. Get current count (default to 0)
    current_count = await ctx.state.get(word, 0)
    
    # 2. Update state
    await ctx.state.put(word, current_count + 1)

## Windowing Strategies

PSPF provides built-in support for windowed aggregations:

- **Tumbling Windows**: Fixed-size, non-overlapping intervals (e.g., every 5 minutes).
- **Sliding Windows**: Overlapping intervals (e.g., 5-minute window sliding every 1 minute).
- **Session Windows**: Dynamic windows defined by activity gaps.
    - **Stateful Merging**: Unlike fixed windows, sessions grow as new events arrive. If an event arrives within the configured `gap_ms`, it is merged into the current session.

```python
from pspf.processing.windows import SessionWindow

# Define a session with a 10-second inactivity gap
@stream.window("user_activity", SessionWindow(gap_ms=10000))
async def track_session(event, current_state):
    # Logic to aggregate session data
    return (current_state or 0) + 1
```
```

## Configuring the Processor

When initializing your `Stream`, you can provide a `state_store` to the underlying `BatchProcessor`.

```python
from pspf.state.backends.sqlite import SQLiteStateStore
from pspf.processor import BatchProcessor

store = SQLiteStateStore(path="data/state.db")
processor = BatchProcessor(backend, state_store=store)
# ...
```

## Interactive Queries (REST API)

PSPF allows you to query the state stores of live workers using a built-in REST API. This is useful for building dashboards or exposing real-time service data.

### 1. Enable the Admin API
The Admin API is enabled by default in the `BatchProcessor`. You can configure the port via the `admin_port` setting.

### 2. Distributed Query Routing
The Admin API is transparently distributed. If you query a key that is managed by a different node in the cluster, the Admin API will:
1. Resolve the leader node for that key via the `ClusterCoordinator`.
2. Proxy the request to that node.
3. Return the merged result to you.

```bash
# Query any node; it will route to the owner automatically
curl http://any-worker:8001/state/user_id_123
```

**Response:**
```json
{
  "key": "user_id_123",
  "value": {
    "points": 1500,
    "tier": "gold"
  }
}
```

## Checkpointing & EOS

For production reliability, PSPF uses **Atomic Checkpointing**. This means the stream offset (where the worker is in the log) is stored in the *same* database as your state. 

- When a transaction commits, both your `ctx.state.put()` calls and the current message offset are saved together.
- This provides **Exactly-Once Semantics (EOS)** even if the application process or the database connection is interrupted.

