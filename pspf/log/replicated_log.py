import httpx
import asyncio
from typing import Optional, List, AsyncIterator, Dict, Any
from pspf.log.interfaces import Log
from pspf.log.local_log import LocalLog
from pspf.models import StreamRecord
from pspf.cluster.coordinator import ClusterCoordinator
from pspf.utils.logging import get_logger

logger = get_logger("ReplicatedLog")

class ReplicatedLog(Log):
    """
    Wraps a LocalLog and adds synchronous replication logic.
    """
    def __init__(self, local_log: LocalLog, coordinator: ClusterCoordinator, admin_port: int = 8001):
        self._local = local_log
        self._coordinator = coordinator
        self._http_client = httpx.AsyncClient(timeout=2.0)
        self._admin_port = admin_port
        self._running = False
        self._sync_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start the background sync loop for followers."""
        self._running = True
        self._sync_task = asyncio.create_task(self._sync_loop())
        logger.info("ReplicatedLog sync loop started.")

    async def stop(self) -> None:
        """Stop the background sync loop."""
        self._running = False
        self._stop_event.set()
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        await self._http_client.aclose()
        logger.info("ReplicatedLog sync loop stopped.")

    async def _sync_loop(self) -> None:
        """Continuously checks leadership and pulls from leader if follower."""
        while self._running:
            try:
                for p in range(self.partitions()):
                    is_leader = await self._coordinator.try_acquire_leadership(str(p))
                    if not is_leader:
                        # Follower mode: find leader and sync
                        leader_node = await self._coordinator.get_leader_node(str(p))
                        if leader_node:
                            await self._pull_from_leader(p, leader_node)
            except Exception as e:
                logger.error(f"Error in replication sync loop: {e}")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass # Continue loop

    async def _pull_from_leader(self, partition: int, leader_node: Dict[str, Any]) -> None:
        from pspf.models import StreamRecord
        url = f"http://{leader_node['host']}:{leader_node['port']}/internal/pull/{partition}"
        try:
            # We need to know our high watermark (offset) to resume pulling
            offset = await self.get_high_watermark(partition)
            
            resp = await self._http_client.get(f"{url}?offset={offset}")
            resp.raise_for_status()
            
            records = resp.json()
            if records:
                logger.debug(f"Pulled {len(records)} records from leader {leader_node['id']} for partition {partition}")
            for r_dict in records:
                r = StreamRecord(**r_dict)
                # Append locally (bypass replication check)
                await self.append_follower(r)
        except Exception as e:
            logger.debug(f"Failed to pull from leader {leader_node['id']} for partition {partition}: {e}")

    def partitions(self) -> int:
        return self._local.partitions()

    async def get_high_watermark(self, partition: int) -> int:
        return await self._local.get_high_watermark(partition)

    async def read(self, partition: int, offset: int) -> AsyncIterator[StreamRecord]:
        async for r in self._local.read(partition, offset):
            yield r

    async def append(self, record: StreamRecord) -> None:
        """
        Primary append path (called by Producer/Worker).
        """
        partition = self._local._get_partition(record.key)
        
        # 1. Check Leadership
        is_leader = await self._coordinator.try_acquire_leadership(str(partition))
        
        if not is_leader:
            raise Exception(f"Not leader for partition {partition}")

        # 2. Write Locally (WAL)
        await self._local.append(record)
        
        # 3. Synchronous Replication
        others = await self._coordinator.get_other_nodes()
        if not others:
            return # No one else to replicate to (Single Node Cluster)

        # Broadcast to all others (Fan-out)
        # In a real system we'd check ACK quorum (N/2 + 1)
        # Here we just try to send to all, log errors
        
        tasks = []
        for node in others:
            tasks.append(self._replicate_to_node(node, record))
            
        await asyncio.gather(*tasks, return_exceptions=True)
        # TODO: Handle failures? For now "Best Effort" synchronous replication

    async def _replicate_to_node(self, node: Dict[str, Any], record: StreamRecord) -> None:
        url = f"http://{node['host']}:{node['port']}/internal/replicate" # Port? Admin port?
        # Coordinator stores registered port. If that's the Admin port, good. 
        # If it's the Prometheus port, bad.
        # We need to ensure nodes register their ADMIN port. 
        # Assuming they register the correct port.
        
        try:
            # Need to serialize record to JSON compatible dict
            payload = record.model_dump(mode='json')
            resp = await self._http_client.post(url, json=payload)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to replicate to {node['id']} ({url}): {e}")

    async def append_follower(self, record: StreamRecord) -> None:
        """
        Called by the replication endpoint.
        """
        # Write directly to local log without leadership check
        await self._local.append(record)
