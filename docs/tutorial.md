# Tutorial: Building an Order Processor

In this tutorial, we will build a production-ready **Order Processing Service** using PSPF. 
We will create a **Producer** that emits orders and a **Consumer** that processes them asynchronously.

## Prerequisites

- Python 3.9+
- Valkey (or Redis) running on `localhost:6379`
- `pspf` installed

```bash
pip install pspf
```

## Step 1: Define the Event Schema

First, we define what an "Order" looks like. We use Pydantic models for type safety and validation.

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

This is the core service that receives orders and "processes" them (e.g., ships items, charges credit cards).

Create a file named `consumer.py`:

```python
# consumer.py
import asyncio
from pspf import Stream, ValkeyConnector, ValkeyStreamBackend
from schema import OrderCreated

async def process_order(event: OrderCreated):
    """
    This is your business logic. 
    It is called automatically for every new event.
    """
    print(f"📦 [START] Processing Order {event.order_id} for User {event.user_id}")
    
    # Simulate some work (e.g. calling Stripe API)
    await asyncio.sleep(0.5) 
    
    print(f"✅ [DONE ] Shipped {event.quantity}x {event.sku}")

async def main():
    # 1. Configure the Backend (Valkey/Redis)
    connector = ValkeyConnector(host='localhost', port=6379)
    
    # 2. Setup the Stream Backend
    # stream_key: "orders"
    # group_name: "fulfillment-service" (All workers share this)
    # consumer_name: "worker-1" (Unique ID for this instance)
    backend = ValkeyStreamBackend(connector, 
                                  stream_key="orders", 
                                  group_name="fulfillment-service", 
                                  consumer_name="worker-1")

    # 3. Start the Stream Processing Loop
    print("🚀 Fulfillment Service Started...")
    async with Stream(backend, schema=OrderCreated) as stream:
        # Pass your handler function. PSPF handles the rest.
        await stream.run(process_order)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

## Step 3: Create the Producer

Now we need a way to send orders into the system. In a real app, this might be your FastAPI or Django web server handling a "Checkout" request.

Create a file named `producer.py`:

```python
# producer.py
import asyncio
import uuid
import random
from pspf import Stream, ValkeyConnector, ValkeyStreamBackend
from schema import OrderCreated

async def main():
    connector = ValkeyConnector(host='localhost', port=6379)
    # Producers don't need a group_name or consumer_name really, but the backend requires them for init currently.
    # You can reuse the same backend class or we could genericize it.
    backend = ValkeyStreamBackend(connector, "orders", "producer-group", "producer")

    async with Stream(backend, schema=OrderCreated) as stream:
        print("📤 Sending orders...")
        
        for i in range(5):
            # Create a valid order
            order = OrderCreated(
                order_id=str(uuid.uuid4()),
                user_id=f"user-{random.randint(100, 999)}",
                sku="WIDGET-X",
                quantity=random.randint(1, 5),
                amount=99.99
            )
            
            # Emit to Stream
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
*You should see:* `🚀 Fulfillment Service Started...`

**Terminal 2: Run the Producer**
```bash
python producer.py
```

### Results

You will see orders appearing in the Consumer window:

```text
📦 [START] Processing Order 67b... for User user-123
✅ [DONE ] Shipped 2x WIDGET-X
```

## Next Steps

1. **Scale It**: Open a third terminal and run `python consumer.py` again (change `consumer_name="worker-2"` in code or env vars). PSPF will automatically load-balance orders between worker-1 and worker-2.
2. **Crash It**: Kill a worker while it's processing. Restart it. Notice how PSPF reclaims the stuck message and ensures it gets processed!
3. **Visualize It**: Open `http://localhost:8000` (if you enabled Prometheus) to see metrics.
