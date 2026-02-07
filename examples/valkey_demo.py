import asyncio
import logging
from pspf import Stream, BaseEvent
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.settings import settings
from pydantic import Field

# 1. Define a Schema
class UserSignupEvent(BaseEvent):
    user_id: str
    email: str
    campaign_source: str = "direct"

# 2. Define logic
async def process_user_signup(event: UserSignupEvent):
    print(f"[{event.offset}] Processing signup for user: {event.user_id} ({event.email})")
    # Simulate work
    await asyncio.sleep(0.01)

# 3. Run Pipeline
async def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # "Dependency Injection" style config
    stream_name = "events:user_signups"
    group_name = "processor-group-1"
    consumer_name = "worker-1"
    
    # Initialize the Backend using Settings
    connector = ValkeyConnector(
        host=settings.VALKEY_HOST, 
        port=settings.VALKEY_PORT,
        password=settings.VALKEY_PASSWORD
    )
    backend = ValkeyStreamBackend(connector, stream_name, group_name, consumer_name)
    
    # Initialize the Stream Facade
    async with Stream(backend, schema=UserSignupEvent) as stream:
        params = await stream.health()
        print(f"Stream Health: {params}")
        
        # Produce some test data (In a real app, this would be in a separate producer)
        print("Producing test events...")
        for i in range(5):
            evt = UserSignupEvent(
                event_type="UserSignupEvent",
                user_id=f"user_{i}", 
                email=f"user{i}@example.com"
            )
            msg_id = await stream.emit(evt)
            print(f"Produced event {msg_id}")

        # Start Processing
        print("Starting processor (Press Ctrl+C to stop)...")
        # In a real app, this runs indefinitely. 
        # Here we rely on the loop running until we stop it or it catches the signal.
        # For demo purposes, we might just run it for a few seconds if we wanted to auto-exit.
        await stream.run(handler=process_user_signup, batch_size=2)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
