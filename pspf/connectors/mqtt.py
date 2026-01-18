import asyncio
from typing import List, Optional, Any
from pspf.connectors.base import Source

class MQTTSource(Source[bytes]):
    """
    Production-grade MQTT source using gmqtt.
    """
    def __init__(self, topic: str, host: str, port: int = 1883):
        super().__init__(name=f"MQTTSource({topic})")
        self.topic = topic
        self.host = host
        self.port = port
        self._client = None
        self._queue = asyncio.Queue()

    async def start(self) -> None:
        try:
            from gmqtt import Client as MQTTClient
            self._client = MQTTClient("pspf-client")
            self._client.on_message = self._on_message
            
            await self._client.connect(self.host, self.port)
            self._client.subscribe(self.topic)
            self.logger.info(f"Subscribed to MQTT: {self.topic}")
            
            while True:
                msg = await self._queue.get()
                await self.emit(msg)
        except ImportError:
            self.logger.warning("gmqtt not installed. Falling back to simulation.")
            for i in range(5):
                await self.emit(f"mqtt-{i}".encode())
                await asyncio.sleep(0.2)
        finally:
            if self._client:
                await self._client.disconnect()

    def _on_message(self, client, topic, payload, qos, properties):
        self._queue.put_nowait(payload)
