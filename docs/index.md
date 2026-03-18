# Quick Start

Welcome to the Python Stream Processing Framework (PSPF). PSPF provides a robust, developer-friendly way to build event-driven applications with Python and Valkey.

## Installation

```bash
pip install pspf
```

## Core Concepts

PSPF is built on **Composition**. You define a **Schema**, choose a **Backend**, and pass them to a **Stream** facade.

```python
# 1. Define your Data Model
class UserSignup(BaseEvent):
    user_id: str
    email: str

# 2. Define Stream and Handlers
# (PSPF auto-connects to Valkey or falls back to Memory Backend)
stream = Stream(topic="signups", group="signup-workers")

@stream.subscribe("signups", schema=UserSignup)
async def handle_signup(event: UserSignup):
    print(f"Processing signup for {event.email}")

async def main():
    async with stream:
        # Produce an event
        await stream.emit(UserSignup(user_id="123", email="joe@example.com"))
        
        # Run Consumer concurrently
        await stream.run_forever()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

> **Note**: While PSPF defaults to Valkey via auto-instantiation, it is completely backend-agnostic! You can explicitly swap it out with `KafkaConnector` or `MemoryBackend` by passing a custom `backend` to the `Stream`.

## Key Features
- **Backend-Agnostic**: Run on Valkey, Kafka, Memory, or local Files.
- **Type Safety**: Full Pydantic V2 integration for all events.
- **Reliability**: Automatic message recovery and Dead Letter Queues.
- **Observability**: Built-in Prometheus metrics and OpenTelemetry tracing.
- **Scalability**: Seamless horizontal scaling via Consumer Groups.
