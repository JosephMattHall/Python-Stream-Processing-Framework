import asyncio
import time
from pspf.stream import Stream
from pspf.settings import settings
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.state.backends.valkey_store import ValkeyStateStore
from pspf.processing.windows import SessionWindow

async def main():
    # Disable telemetry to avoid port conflicts in Docker
    settings.telemetry.ENABLED = False
    
    connector = ValkeyConnector(settings.valkey.HOST, settings.valkey.PORT)
    await connector.connect()
    
    state_store = ValkeyStateStore(connector)
    await state_store.start()
    
    backend = ValkeyStreamBackend(
        connector, 
        stream_key="session_test", 
        group_name="session_group", 
        consumer_name="session_worker"
    )
    stream = Stream(backend, state_store=state_store)
    
    # 5-second gap session
    @stream.window("session_test", SessionWindow(gap_ms=5000))
    async def aggregate_counts(event, current_state):
        count = (current_state or 0) + 1
        return count

    # Clear old state
    await connector.get_client().delete("session_test:user1:session:active")
    
    # Start processor
    task = asyncio.create_task(stream.run_forever())
    await asyncio.sleep(1)
    
    print("Producing events for user1...")
    # Event 1
    await stream.emit({"key": "user1", "val": 1})
    await asyncio.sleep(2)
    
    # Event 2 (2s later, within 5s gap)
    await stream.emit({"key": "user1", "val": 1})
    await asyncio.sleep(1)
    
    print("Checking state during session...")
    state = await connector.get_client().get("session_test:user1:session:active")
    print(f"Intermediate Session State: {state}")
    
    # Event 3 (1s later, within gap)
    await stream.emit({"key": "user1", "val": 1})
    await asyncio.sleep(6) # Wait for session to 'expire' in terms of gap
    
    # Event 4 (6s later, outside gap)
    await stream.emit({"key": "user1", "val": 1})
    await asyncio.sleep(1)
    
    print("Checking final state...")
    final_state = await connector.get_client().get("session_test:user1:session:active")
    print(f"Final Session State: {final_state}")
    
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await connector.close()

if __name__ == "__main__":
    asyncio.run(main())
