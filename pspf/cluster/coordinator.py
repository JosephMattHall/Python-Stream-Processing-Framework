import asyncio
import uuid
import time
import json
from typing import Optional, List, Dict, Any
from pspf.utils.logging import get_logger
from pspf.cluster.interface import ClusterCoordinator as IClusterCoordinator
import valkey.asyncio as valkey

logger = get_logger("ClusterCoordinator")

class ValkeyClusterCoordinator(IClusterCoordinator):
    """
    Valkey implementation of ClusterCoordinator.
    
    Keys:
    - pspf:nodes:<node_id> -> Metadata (TTL 10s)
    - pspf:partition:<key>:leader -> Node ID (TTL 10s)
    """
    def __init__(self, valkey_url: str, host: str, port: int, node_id: Optional[str] = None) -> None:
        self.valkey_url = valkey_url
        self.host = host
        self.port = port
        self.node_id = node_id or str(uuid.uuid4())
        self._running = False
        self._client: Optional[valkey.Redis] = None
        self._held_partitions: List[str] = []
        
    async def start(self) -> None:
        self._client = valkey.from_url(self.valkey_url, decode_responses=True)
        self._running = True
        logger.info(f"Starting Coordinator for Node {self.node_id} ({self.host}:{self.port})")
        
        # Initial Registration
        await self._register()
        
        # Start Heartbeat Loop
        asyncio.create_task(self._heartbeat_loop())
        
    async def stop(self) -> None:
        self._running = False
        if self._client:
            # Release leaderships? Or let them expire.
            # Ideally release for fast failover.
            for p_key in self._held_partitions:
                await self._client.delete(f"pspf:partition:{p_key}:leader")
            await self._client.close()

    async def _register(self) -> None:
        if not self._client: return
        data = {
            "id": self.node_id,
            "host": self.host,
            "port": self.port,
            "started_at": time.time()
        }
        # Set with TTL 10s
        await self._client.set(f"pspf:nodes:{self.node_id}", json.dumps(data), ex=10)

    async def _heartbeat_loop(self) -> None:
        if not self._client: return
        while self._running:
            try:
                # 1. Refresh Node TTL
                await self._register()
                
                # 2. Refresh Leases for held partitions
                for p_key in self._held_partitions:
                    # Extend TTL only if we are still the owner
                    key = f"pspf:partition:{p_key}:leader"
                    script = """
                    if redis.call("get", KEYS[1]) == ARGV[1] then
                        return redis.call("expire", KEYS[1], 10)
                    else
                        return 0
                    end
                    """
                    result = await self._client.eval(script, 1, key, self.node_id) # type: ignore
                    if not result:
                        logger.warning(f"Lost leadership for {p_key}")
                        self._held_partitions.remove(p_key)
                        
                # 3. Simple Rebalancing Check
                try:
                    cursor, node_keys = await self._client.scan(0, match="pspf:nodes:*")
                    all_nodes_count = len(node_keys)
                    if all_nodes_count > 1 and self._held_partitions:
                        # We count known partition leader keys to estimate total active partitions
                        cursor, part_keys = await self._client.scan(0, match="pspf:partition:*:leader")
                        total_parts = len(part_keys) 
                        
                        # Add 1 to handle uneven remainders safely
                        fair_share = (total_parts // all_nodes_count) + 1
                        
                        if len(self._held_partitions) > fair_share:
                            # Voluntarily give up one partition lock so an idle node can claim it
                            relinquished_p_key = self._held_partitions.pop(0)
                            logger.info(f"Rebalancing: voluntarily giving up partition {relinquished_p_key} (holding {len(self._held_partitions)+1}, fair share <= {fair_share})")
                            await self._client.delete(f"pspf:partition:{relinquished_p_key}:leader")
                except Exception as e:
                    logger.warning(f"Rebalancing routine encountered an issue: {e}")
                        
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
            
            await asyncio.sleep(3) # Refresh every 3s (well within 10s TTL)

    async def try_acquire_leadership(self, partition_key: str) -> bool:
        """
        Attempts to become the leader for a partition.
        Returns True if successful (acquired or already held).
        """
        if not self._client: return False
        
        key = f"pspf:partition:{partition_key}:leader"
        
        # Try SET NX EX 10
        acquired = await self._client.set(key, self.node_id, nx=True, ex=10)
        
        if acquired:
            if partition_key not in self._held_partitions:
                self._held_partitions.append(partition_key)
            logger.info(f"Acquired leadership for {partition_key}")
            return True
            
        # If not acquired, check if WE are the owner (maybe registered from before restart or same session)
        current_owner = await self._client.get(key)
        if current_owner == self.node_id:
             if partition_key not in self._held_partitions:
                self._held_partitions.append(partition_key)
             return True
             
        return False
        
    async def get_leader_node(self, partition_key: str) -> Optional[Dict[str, Any]]:
        """
        Resolves the leader node metadata for a partition.
        Returns None if no leader.
        """
        if not self._client: return None
        
        leader_id = await self._client.get(f"pspf:partition:{partition_key}:leader")
        if not leader_id:
            return None
            
        node_json = await self._client.get(f"pspf:nodes:{leader_id}") # type: ignore
        if node_json:
            return json.loads(node_json) # type: ignore
        return None

    async def get_other_nodes(self) -> List[Dict[str, Any]]:
        """
        Returns a list of metadata for all OTHER registered nodes (excluding self).
        """
        if not self._client: return []
        
        nodes = []
        # SCAN for pspf:nodes:*
        # Note: SCAN might be slow for huge clusters, but fine for v1.
        # Or maintenance of a Set 'pspf:known_nodes'
        
        # Let's just scan for now or check a known set if we maintained one.
        # But for simplicity, let's just use KEYS (BAD for prod) or SCAN. 
        # Since node count is small (edge), SCAN is fine.
        
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(cursor, match="pspf:nodes:*")
            for k in keys:
                 nid = k.split(":")[-1]
                 if nid == self.node_id:
                     continue
                 
                 data = await self._client.get(k)
                 if data:
                     nodes.append(json.loads(data)) # type: ignore
            
            if cursor == 0:
                break
                
        return nodes

# Alias for backward compatibility
ClusterCoordinator = ValkeyClusterCoordinator
