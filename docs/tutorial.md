# Tutorial: Building an Order Processor

In this tutorial, we will build a production-ready **Order Processing Service** using PSPF. We will create a **Producer** that emits orders and a **Consumer** that processes them asynchronously using **Valkey** as the message broker.

## Prerequisites

- Python 3.9+
- Valkey (or Redis) running on `localhost:6379`
- `pspf` installed via `pip install .`

## Step 1: Define the Event Schema

First, we define what an "Order" looks like. We use Pydantic models (extending `BaseEvent`) for type safety and validation.

Create a file named `schema.py`:

```python
# schema.py
from pspf import BaseEvent
from pydantic import Field

class OrderCreated(BaseEvent):
    order_id: str
    user_id: str
    sku: str
    quantity: int = Field(gt=0)
    amount: float
    currency: str = "USD"
```

## Step 2: Create the Consumer (Worker)

This is the service that receives orders and processes them (e.g., shipping items, charging cards).

Create a file named `consumer.py`:

```python
# consumer.py
import asyncio
from pspf import Stream
from schema import OrderCreated

async def main():
    # 1. Define the Stream
    # (PSPF will automatically connect to Valkey since it's the default)
    stream = Stream(topic="orders", group="fulfillment-service")

    # 2. Register the Handler
    @stream.subscribe("orders", schema=OrderCreated)
    async def process_order(event: OrderCreated):
        print(f"📦 [START] Processing Order {event.order_id} for User {event.user_id}")
        await asyncio.sleep(0.5) 
        print(f"✅ [DONE ] Shipped {event.quantity}x {event.sku}")


    # 5. Start the Stream Processing Loop
    print("🚀 Fulfillment Service Started...")
    async with stream:
        await stream.run_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...")
```

## Step 3: Create the Producer

Now we need to send orders into the system. This normally happens in your web API or other services.

Create a file named `producer.py`:

```python
# producer.py
import asyncio
import uuid
from pspf import Stream
from schema import OrderCreated

async def main():
    async with Stream(topic="orders", group="producer-group", schema=OrderCreated) as stream:
        print("📤 Sending orders...")
        for i in range(5):
            order = OrderCreated(
                order_id=str(uuid.uuid4())[:8],
                user_id=f"user-{i}",
                sku="WIDGET-X",
                quantity=1,
                amount=99.99
            )
            msg_id = await stream.emit(order)
            print(f"   -> Sent {order.order_id} (ID: {msg_id})")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
```

## Step 4: Run the Application

Open **two terminal windows**.

**Terminal 1: Run the Consumer**
```bash
python consumer.py
```

**Terminal 2: Run the Producer**
```bash
python producer.py
```

### Results
You will see orders appearing in the Consumer window as they are emitted by the Producer.

---

## Alternative: Testing Locally with MemoryBackend

If you don't have Valkey or Redis running or just want to quickly test the application logic entirely in memory, you can swap the backend in a single file approach.

Create a file named `memory_tutorial.py`:

```python
import asyncio
import uuid
from pspf import Stream
from pspf.connectors.memory import MemoryBackend
from schema import OrderCreated

async def main():
    # 1. Use the MemoryBackend explicitly
    backend = MemoryBackend(stream_key="orders", group_name="local-test-group")
    stream = Stream(backend=backend)

    # 2. Register the Handler
    @stream.subscribe("orders", schema=OrderCreated)
    async def process_order(event: OrderCreated):
        print(f"📦 [START] Processing Order {event.order_id} for User {event.user_id}")
        await asyncio.sleep(0.5) 
        print(f"✅ [DONE ] Shipped {event.quantity}x {event.sku}")

    # 3. Start the Stream Processing Loop in the background
    print("🚀 Fulfillment Service Started (In-Memory)...")
    task = asyncio.create_task(stream.run_forever())

    # 4. Produce events in the same script
    print("📤 Sending orders...")
    for i in range(3):
        order = OrderCreated(
            order_id=str(uuid.uuid4())[:8],
            user_id=f"user-{i}",
            sku="WIDGET-TEST",
            quantity=2,
            amount=19.99
        )
        msg_id = await stream.emit(order)
        print(f"   -> Sent {order.order_id} (ID: {msg_id})")
        await asyncio.sleep(1)

    # Clean up
    await stream.stop()
    await task

if __name__ == "__main__":
    asyncio.run(main())
```

Run it with a single command:
```bash
python memory_tutorial.py
```

## Next Steps
1. **Scale**: Run multiple `consumer.py` instances with different `consumer_name` values to see automatic load balancing (Requires Valkey backend).
2. **Recover**: Stop a consumer mid-process and restart it; PSPF will recover the "in-flight" message automatically (Requires persistent backend).
3. **Functional DSL**: For complex transformations, try the `StreamBuilder` API to chain `map` and `filter` operations.
