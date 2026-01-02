import asyncio
from typing import List, Any
from pspf.connectors.base import Source, Sink

class KafkaSource(Source[str]):
    """Simulated Kafka source."""

    def __init__(self, topic: str, bootstrap_servers: List[str]):
        super().__init__(name=f"KafkaSource({topic})")
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    async def start(self) -> None:
        # Simulation: infinite stream of messages
        i = 0
        while i < 10:  # Limit for testing to avoid infinite loops
            await self.emit(f"message-{i} from {self.topic}")
            await asyncio.sleep(0.1)
            i += 1


class KafkaSink(Sink[Any]):
    """Simulated Kafka sink."""

    def __init__(self, topic: str, bootstrap_servers: list[str]):
        super().__init__(name=f"KafkaSink({topic})")
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    async def _process_captured(self, element: Any) -> None:
        print(f"[KafkaSink] Writing to {self.topic}: {element}")
