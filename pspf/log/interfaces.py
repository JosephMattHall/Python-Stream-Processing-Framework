from abc import ABC, abstractmethod
from typing import AsyncIterator, List
from pspf.models import StreamRecord

class Log(ABC):
    """
    Abstract interface for an append-only event log.
    This replaces the direct dependency on Kafka.
    """
    
    @abstractmethod
    async def append(self, record: StreamRecord) -> None:
        """Append a record to the log."""
        pass

    @abstractmethod
    async def read(self, partition: int, offset: int) -> AsyncIterator[StreamRecord]:
        """
        Read records from a specific partition starting at the given offset.
        Yields records as they become available (or just the batch).
        """
        pass

    @abstractmethod
    def partitions(self) -> int:
        """Return the number of partitions."""
        pass

class OffsetStore(ABC):
    """
    Abstract interface for managing consumer group offsets.
    """

    @abstractmethod
    async def get(self, consumer_id: str, partition: int) -> int:
        """Get the committed offset for a consumer group and partition."""
        pass

    @abstractmethod
    async def commit(self, consumer_id: str, partition: int, offset: int) -> None:
        """Commit an offset for a consumer group and partition."""
        pass
