from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class ClusterCoordinator(ABC):
    """
    Abstract Base Class for Cluster Coordination.
    Handles node discovery, heartbeats, and partition leadership.
    """
    
    @property
    @abstractmethod
    def node_id(self) -> str:
        pass

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def try_acquire_leadership(self, partition_key: str) -> bool:
        """Attempt to become the leader for a partition."""
        pass

    @abstractmethod
    async def get_leader_node(self, partition_key: str) -> Optional[Dict[str, Any]]:
        """Resolve the metadata for the leader of a partition."""
        pass

    @abstractmethod
    async def get_other_nodes(self) -> List[Dict[str, Any]]:
        """List all other active nodes in the cluster."""
        pass
