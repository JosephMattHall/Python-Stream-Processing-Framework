import asyncio
from pydantic import BaseModel
from pspf.stream import Stream
from pspf.connectors.memory import MemoryBackend
from pspf.topology import Router

class Transaction(BaseModel):
    id: str
    amount: float
    user_id: str

async def main() -> None:
    # Setup the stream and router
    backend = MemoryBackend(stream_key="transactions", group_name="demo-group")
    stream: Stream[Transaction] = Stream(backend=backend, schema=Transaction)
    router = Router(stream)
    
    # 1. Add routing rules
    router.add_route(lambda tx: tx.amount > 10000, target_topic="high_value_tx")
    router.add_route(lambda tx: tx.amount <= 10000, target_topic="standard_tx")
    
    # 2. Subscribe to the sub-topics
    @stream.subscribe("high_value_tx")
    async def process_high_value(tx: Transaction) -> None:
        print(f"🚨 ALERT: High value transaction {tx.id} for ${tx.amount}")
        
    @stream.subscribe("standard_tx")
    async def process_standard(tx: Transaction) -> None:
        print(f"✅ Processed standard transaction {tx.id} for ${tx.amount}")
        
    # Start the processing loops in the background
    task = asyncio.create_task(stream.run_forever())
    
    # Wait for connections to establish
    await stream.backend.connect()
    
    # Simulate incoming events on the main input stream
    events = [
        Transaction(id="tx-1", amount=50.0, user_id="u1"),
        Transaction(id="tx-2", amount=15000.0, user_id="u2"),
        Transaction(id="tx-3", amount=500.0, user_id="u3"),
    ]
    
    for e in events:
        await router.route(e)
        
    await asyncio.sleep(1) # Let the processors catch up
    
    # Clean up
    task.cancel()
    try:
        await stream.stop()
        await task
    except asyncio.CancelledError:
        pass
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
