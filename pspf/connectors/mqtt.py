import asyncio
from typing import List, Optional
from pspf.connectors.base import Source

class MQTTSource(Source[str]):

    def __init__(self, topic: str, host: str, port: int = 1883, msg_limit: Optional[int] = 5):
        super().__init__(name=f"MQTTSource({topic})")
        self.topic = topic
        self.host = host
        self.port = port
        self.msg_limit = msg_limit

    async def start(self) -> None:
        """Starts the simulated MQTT message stream."""
        i = 0
        while True:
            if self.msg_limit is not None and i >= self.msg_limit:
                break
            
            await self.emit(f"mqtt-msg-{i} on {self.topic}")
            await asyncio.sleep(0.2)
            i += 1
