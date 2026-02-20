# Quick Start

Welcome to the Python Stream Processing Framework (PSPF). PSPF provides a robust, developer-friendly way to build event-driven applications with Python and Valkey.

## Installation

```bash
pip install .
```

## Core Concepts

PSPF is built on **Composition**. You define a **Schema**, choose a **Backend**, and pass them to a **Stream** facade.

```python
import asyncio
from pspf import Stream, BaseEvent, ValkeyConnector, ValkeyStreamBackend

# 1. Define your Data Model
class UserSignup(BaseEvent):
    user_id: str
    email: str

# 2. Define your Handler
async def process_signup(event: UserSignup):
    print(f"Processing signup for {event.email}")

async def main():
    # 3. Configure Backend (Requires Valkey/Redis)
    connector = ValkeyConnector(host="localhost", port=6379)
    backend = ValkeyStreamBackend(connector, stream_key="signups", group_name="signup-workers")

    # 4. Start Stream
    async with Stream(backend, schema=UserSignup) as stream:
        # Produce an event
        await stream.emit(UserSignup(user_id="123", email="joe@example.com"))
        
        # Run Consumer
        await stream.run(process_signup)

if __name__ == "__main__":
    asyncio.run(main())
```

## Key Features
- **Type Safety**: Full Pydantic V2 integration for all events.
- **Reliability**: Automatic message recovery and Dead Letter Queues.
- **Observability**: Built-in Prometheus metrics and OpenTelemetry tracing.
- **Scalability**: Seamless horizontal scaling via Consumer Groups.
