import asyncio
import httpx
from pspf.schema import BaseEvent
from pspf.connectors.base import BaseSink
from pspf.state.store import StateStore
from pspf.utils.logging import get_logger

logger = get_logger("HttpSink")

class HttpSink(BaseSink):
    """
    An idempotent HTTP Sink that sends events to a remote URL.
    Enforces idempotency using the standard BaseSink logic.
    """
    def __init__(self, name: str, state_store: StateStore, url: str, timeout: float = 5.0):
        super().__init__(name, state_store)
        self.url = url
        self.timeout = timeout
        self.client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        await super().start()
        self.client = httpx.AsyncClient(timeout=self.timeout)

    async def stop(self) -> None:
        if self.client:
            await self.client.aclose()
        await super().stop()

    async def on_write(self, event: BaseEvent, idempotency_token: str) -> None:
        """
        Send the event via POST, including the idempotency token in the headers.
        """
        if not self.client:
            raise RuntimeError("Sink not started")

        payload = event.model_dump(mode='json')
        headers = {
            "X-Idempotency-Key": idempotency_token,
            "Content-Type": "application/json"
        }

        logger.info(f"Sending event {event.event_id} to {self.url} (Token: {idempotency_token})")
        
        response = await self.client.post(self.url, json=payload, headers=headers)
        
        # We raise for status so the processor knows if it failed (and should retry)
        # If it succeeds, BaseSink will then record the token in StateStore.
        response.raise_for_status()

# Demo Usage
async def demo():
    from pspf.state.backends.memory_store import InMemoryStateStore
    
    store = InMemoryStateStore()
    sink = HttpSink("my-api", store, "https://api.example.com/webhooks")
    
    await sink.start()
    
    event = BaseEvent(event_type="Test", payload={"foo": "bar"})
    
    try:
        # Note: This will fail in this demo because the URL is fake, 
        # but it shows the pattern.
        await sink.write(event)
    except Exception as e:
        print(f"Expected failure for fake URL: {e}")
    
    await sink.stop()

if __name__ == "__main__":
    asyncio.run(demo())
