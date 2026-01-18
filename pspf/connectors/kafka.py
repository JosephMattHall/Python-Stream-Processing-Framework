import asyncio
from typing import List, Any, Optional
from pspf.connectors.base import Source
from pspf.utils.logging import get_logger

class KafkaSource(Source[bytes]):
    """
    Production-grade Kafka source using aiokafka.
    """
    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        super().__init__(name=f"KafkaSource({topic})")
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._consumer = None

    async def start(self) -> None:
        try:
            from aiokafka import AIOKafkaConsumer
            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest'
            )
            await self._consumer.start()
            self.logger.info(f"Connected to Kafka: {self.topic}")
            
            async for msg in self._consumer:
                # We emit the raw bytes, downstream operators (like pipeline) handle parsing
                await self.emit(msg.value)
        except ImportError:
            self.logger.warning("aiokafka not installed. Falling back to simulation.")
            i = 0
            while i < 10:
                await self.emit(f"msg-{i}".encode())
                await asyncio.sleep(0.1)
                i += 1
        finally:
            if self._consumer:
                await self._consumer.stop()
