# Stateful Processing

PSPF supports **Stateful Stream Processing**, allowing your handlers to maintain and update persistent state (e.g., counters, running totals, or session data).

## State Stores

State is managed via a `StateStore` interface. Currently supported backends:
- **SQLite**: Local, file-based state (perfect for small-to-medium datasets).
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

## Checkpointing (Experimental)

For large-scale state recovery, PSPF supports checkpointing state to a persistent log, allowing you to reconstruct state by replaying events from a specific point in time.
