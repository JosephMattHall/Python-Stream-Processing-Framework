# Quick Start

Welcome to the Python Stream Processing Framework (PSPF).

## Installation

```bash
pip install pspf
```

## Usage

We use Dependency Injection (Composition) for flexibility and testability.

```python
import asyncio
from pspf import Stream, BaseEvent
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pydantic import Field

# 1. Define your Data Model
class UserSignup(BaseEvent):
    user_id: str
    email: str

# 2. Define your Handler
async def process_signup(event: UserSignup):
    print(f"Processing signup for {event.email}")
    await asyncio.sleep(0.01)

async def main():
    # 3. Configure Backend
    connector = ValkeyConnector(host="localhost", port=6379)
    backend = ValkeyStreamBackend(connector, "signups", "email-workers", "worker-1")

    # 4. Start Stream
    async with Stream(backend, schema=UserSignup) as stream:
        
        # Produce an event
        await stream.emit(UserSignup(user_id="123", email="joe@example.com"))
        
        # Run Consumer
        await stream.run(process_signup)

if __name__ == "__main__":
    asyncio.run(main())
```

## Features
- **Type Safety**: Full Pydantic V2 integration.
- **Reliability**: Auto-claims crashed worker messages.
- **Observability**: Built-in OpenTelemetry tracing and Prometheus metrics.
