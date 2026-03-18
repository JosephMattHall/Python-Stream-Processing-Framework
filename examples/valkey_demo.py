import asyncio
import logging
from pspf import Stream, BaseEvent
from pydantic import Field

# 1. Define a Schema
class UserSignupEvent(BaseEvent):
    user_id: str
    email: str
    campaign_source: str = "direct"

# 2. Define logic
async def process_user_signup(event: UserSignupEvent) -> None:
    print(f"[{event.offset}] Processing signup for user: {event.user_id} ({event.email})")
    # Simulate work
    await asyncio.sleep(0.01)

# 3. Run Pipeline
async def main() -> None:
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create Stream with auto-instantiation
    # It will use Valkey if VALKEY_HOST is set, or fall back to Memory in dev.
    stream = Stream(
        topic="events:user_signups_v3",
        group="signup-processors",
        schema=UserSignupEvent
    )
    
    async with stream:
        # Produce some test data
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
        # run_forever handles both stateless and stateful handlers.
        # For a single simple handler, we can use run().
        await stream.run(handler=process_user_signup, batch_size=2)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
