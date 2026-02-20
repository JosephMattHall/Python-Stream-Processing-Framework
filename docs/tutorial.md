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
from pspf import Stream, ValkeyConnector, ValkeyStreamBackend
from schema import OrderCreated

async def process_order(event: OrderCreated):
    """
    Business logic called for every new event.
    """
    print(f"📦 [START] Processing Order {event.order_id} for User {event.user_id}")
    await asyncio.sleep(0.5) # Simulate work
    print(f"✅ [DONE ] Shipped {event.quantity}x {event.sku}")

async def main():
    # 1. Configure the Connector
    connector = ValkeyConnector(host='localhost', port=6379)
    
    # 2. Setup the Stream Backend
    backend = ValkeyStreamBackend(
        connector, 
        stream_key="orders", 
        group_name="fulfillment-service", 
        consumer_name="worker-1"
    )

    # 3. Start the Stream Processing Loop
    print("🚀 Fulfillment Service Started...")
    async with Stream(backend, schema=OrderCreated) as stream:
        await stream.run(process_order)

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
from pspf import Stream, ValkeyConnector, ValkeyStreamBackend
from schema import OrderCreated

async def main():
    connector = ValkeyConnector(host='localhost', port=6379)
    # Producers use the same backend but typically don't need unique consumer names
    backend = ValkeyStreamBackend(connector, "orders", "producer-group", "producer")

    async with Stream(backend, schema=OrderCreated) as stream:
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

## Next Steps
1. **Scale**: Run multiple `consumer.py` instances with different `consumer_name` values to see automatic load balancing.
2. **Recover**: Stop a consumer mid-process and restart it; PSPF will recover the "in-flight" message automatically.
