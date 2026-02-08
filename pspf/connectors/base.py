from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any, Optional

class Source(ABC):
    """Abstract base class for data sources."""
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

class Sink(ABC):
    """Abstract base class for data sinks."""
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

class StreamingBackend(ABC):
    """
    Abstract Base Class for all Streaming Backends.
    
    Any new backend (Kafka, Pulsar, Memory, etc.) must implement these methods.
    """
    
    @property
    @abstractmethod
    def stream_key(self) -> str:
        """Name of the stream/topic."""
        pass

    @property
    @abstractmethod
    def group_name(self) -> str:
        """Name of the consumer group."""
        pass

    @abstractmethod
    async def connect(self):
        """Establish connection to the backend."""
        pass
        
    @abstractmethod
    async def close(self):
        """Close connection to the backend."""
        pass

    @abstractmethod
    async def ping(self):
        """Check connection health."""
        pass

    @abstractmethod
    async def ensure_group_exists(self, start_id: str = "0"):
        """Ensure the consumer group exists."""
        pass

    @abstractmethod
    async def read_batch(self, count: int = 10, block_ms: int = 1000) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Read a batch of messages.
        
        Returns:
            List of (message_id, data_dict) tuples.
        """
        pass

    @abstractmethod
    async def ack_batch(self, message_ids: List[str]):
        """Acknowledge a batch of messages as processed."""
        pass

    @abstractmethod
    async def add_event(self, data: Dict[str, Any], max_len: Optional[int] = None) -> str:
        """
        Publish an event to the stream.
        
        Returns:
            The generated message ID.
        """
        pass
        
    @abstractmethod
    async def claim_stuck_messages(self, min_idle_time_ms: int = 60000, count: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Recover messages that have been processing for too long.
        """
        pass
        
    @abstractmethod
    async def increment_retry_count(self, message_id: str) -> int:
        """Increment and return the retry count for a message."""
        pass
        
    @abstractmethod
    async def move_to_dlq(self, message_id: str, data: Dict[str, Any], error: str):
        """Move a failed message to the Dead Letter Queue."""
        pass
