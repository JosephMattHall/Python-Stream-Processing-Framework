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

# 2. Configure Backend
connector = ValkeyConnector(host="localhost", port=6379)
backend = ValkeyStreamBackend(connector, stream_key="signups", group_name="signup-workers")

# 3. Define Stream and Handlers
stream = Stream(backend=backend)

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
    asyncio.run(main())
```

> **Note**: While the example above uses `ValkeyConnector`, PSPF is backend-agnostic! You can swap it out with `KafkaConnector`, `MemoryBackend` (for testing), or the native `File` backend.

## Key Features
- **Backend-Agnostic**: Run on Valkey, Kafka, Memory, or local Files.
- **Type Safety**: Full Pydantic V2 integration for all events.
- **Reliability**: Automatic message recovery and Dead Letter Queues.
- **Observability**: Built-in Prometheus metrics and OpenTelemetry tracing.
- **Scalability**: Seamless horizontal scaling via Consumer Groups.
