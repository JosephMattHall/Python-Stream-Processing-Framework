import asyncio
from pydantic import BaseModel
from pspf import Stream
from pspf.topology import Router

class Transaction(BaseModel):
    id: str
    amount: float
    user_id: str

async def main() -> None:
    # 1. Setup the stream using auto-instantiation
    # topic is 'transactions', group is 'demo-group'
    stream: Stream[Transaction] = Stream(topic="transactions", group="demo-group", schema=Transaction)
    router = Router(stream)
    
    # 2. Add routing rules
    router.add_route(lambda tx: tx.amount > 10000, target_topic="high_value_tx")
    router.add_route(lambda tx: tx.amount <= 10000, target_topic="standard_tx")
    
    # 3. Subscribe to the sub-topics
    @stream.subscribe("high_value_tx")
    async def process_high_value(tx: Transaction) -> None:
        print(f"🚨 ALERT: High value transaction {tx.id} for ${tx.amount}")
        
    @stream.subscribe("standard_tx")
    async def process_standard(tx: Transaction) -> None:
        print(f"✅ Processed standard transaction {tx.id} for ${tx.amount}")
        
    print("🚀 Starting Advanced Topology Demo...")
    async with stream:
        # 4. Start the processing loops in the background
        # run_forever handles all subscriptions on this stream instance
        task = asyncio.create_task(stream.run_forever())
        
        # 5. Simulate incoming events on the main input stream
        events = [
            Transaction(id="tx-1", amount=50.0, user_id="u1"),
            Transaction(id="tx-2", amount=15000.0, user_id="u2"),
            Transaction(id="tx-3", amount=500.0, user_id="u3"),
        ]
        
        for e in events:
            # Routing publishes to the sub-topics
            await router.route(e)
            
        await asyncio.sleep(2) # Let the processors catch up
        
        # Clean up
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
