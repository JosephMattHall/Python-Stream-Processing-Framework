import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, AsyncGenerator
import valkey.asyncio as valkey
from valkey.exceptions import ResponseError

from pspf.utils.logging import get_logger

logger = get_logger("ValkeyBackend")

class ValkeyConnector:
    """
    Manages the connection pool to a Valkey (or Redis) server.

    Attributes:
        host (str): Database host.
        port (int): Database port.
        password (Optional[str]): Database password.
        db (int): Database index.
    """
    def __init__(self, host: str = 'localhost', port: int = 6379, password: Optional[str] = None, db: int = 0):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self._pool: Optional[valkey.Valkey] = None

    async def connect(self):
        """
        Establish a connection pool to the database.
        
        Raises:
             ConnectionError: If connectivity fails.
        """
        if not self._pool:
            from pspf.settings import settings
            self._pool = valkey.Valkey(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                ssl=settings.VALKEY_SSL,
                ssl_ca_certs=settings.VALKEY_SSL_CA_CERTS,
                ssl_cert_reqs=settings.VALKEY_SSL_CERT_REQS,
                decode_responses=True
            )
            # Fail fast if connection is bad
            await self._pool.ping()
            logger.info(f"Connected to Valkey at {self.host}:{self.port} (SSL={settings.VALKEY_SSL})")

    async def close(self):
        """
        Close the connection pool.
        """
        if self._pool:
            await self._pool.aclose()
            logger.info("Closed Valkey connection")
            self._pool = None

    def get_client(self) -> valkey.Valkey:
        """
        Get the active client instance.

        Returns:
            valkey.Valkey: The client instance.

        Raises:
            RuntimeError: If connect() has not been called.
        """
        if not self._pool:
            raise RuntimeError("ValkeyConnector is not connected. Call connect() first.")
        return self._pool

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class ValkeyStreamBackend:
    """
    Handles all Stream-related operations on top of a ValkeyConnector.

    Attributes:
        connector (ValkeyConnector): The connection manager.
        stream_key (str): Name of the stream.
        group_name (str): Consumer group name.
        consumer_name (str): Unique consumer identifier.
    """
    def __init__(self, connector: ValkeyConnector, stream_key: str, group_name: str, consumer_name: str):
        self.connector = connector
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.dlq_stream_key = f"{stream_key}-dlq"
        self.retry_tracker_key = f"pspf:retries:{group_name}:{stream_key}"

    async def ensure_group_exists(self, start_id: str = "0"):
        """
        Idempotent creation of the consumer group.
        
        Args:
            start_id (str): ID to start from. Default is "0" (process all).
        """
        client = self.connector.get_client()
        try:
            # MKSTREAM ensures the stream is created if it doesn't exist
            await client.xgroup_create(self.stream_key, self.group_name, id=start_id, mkstream=True)
            logger.info(f"Created consumer group '{self.group_name}' on stream '{self.stream_key}'")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{self.group_name}' already exists.")
            else:
                raise e

    async def read_batch(self, count: int = 10, block_ms: int = 1000) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Reads a batch of new messages ('&gt;') for this consumer group.
        
        Args:
            count (int): Max messages.
            block_ms (int): Block timeout in milliseconds.

        Returns:
            List[Tuple[str, Dict]]: List of (msg_id, payload).
        """
        client = self.connector.get_client()
        
        try:
            # Structure: [[stream_name, [(msg_id, data), ...]], ...]
            streams = await client.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={self.stream_key: ">"}, # ">" means new messages
                count=count,
                block=block_ms
            )

            if not streams:
                return []

            # We only read one stream, so take the first result
            stream_name, messages = streams[0]
            if not messages:
                return []
                
            return messages
        except Exception as e:
            logger.error(f"Error reading batch: {e}")
            raise

    async def get_retry_count(self, message_id: str) -> int:
        """
        Gets the current retry count for a message ID.
        """
        client = self.connector.get_client()
        count = await client.hget(self.retry_tracker_key, message_id)
        if count:
            return int(count)
        return 0

    async def increment_retry_count(self, message_id: str) -> int:
        """
        Increments retry count for a message by 1.
        
        Args:
            message_id (str): The unique message ID.
            
        Returns:
             int: The new retry count.
        """
        client = self.connector.get_client()
        return await client.hincrby(self.retry_tracker_key, message_id, 1)

    async def move_to_dlq(self, message_id: str, data: Dict[str, Any], error: str):
        """
        Moves a message to the DLQ stream and ACKs it in the main stream.
        
        Args:
             message_id (str): Message ID.
             data (Dict): Message payload.
             error (str): Error description.
        """
        client = self.connector.get_client()
        logger.warning(f"Moving message {message_id} to DLQ {self.dlq_stream_key}. Error: {error}")
        
        # Enrich data with error metadata
        dlq_data = data.copy()
        dlq_data["_error"] = str(error)
        dlq_data["_original_stream"] = self.stream_key
        dlq_data["_original_msg_id"] = message_id
        dlq_data["_moved_timestamp"] = str(time.time())
        
        # Add to DLQ
        await client.xadd(self.dlq_stream_key, dlq_data)
        
        # ACK in original stream to remove from Pending Entries List (PEL)
        await client.xack(self.stream_key, self.group_name, message_id)
        
        # Clean up retry tracker
        await client.hdel(self.retry_tracker_key, message_id)

    async def ack_batch(self, message_ids: List[str]):
        """
        Acknowledges a batch of message IDs and cleans up retry counters.
        
        Args:
            message_ids (List[str]): IDs to ACK.
        """
        if not message_ids:
            return
        
        client = self.connector.get_client()
        
        # Pipeline the XACK and retry cleanup
        async with client.pipeline() as pipe:
            pipe.xack(self.stream_key, self.group_name, *message_ids)
            pipe.hdel(self.retry_tracker_key, *message_ids)
            await pipe.execute()
        
        logger.debug(f"ACKed {len(message_ids)} messages")

    async def add_event(self, data: Dict[str, Any], max_len: Optional[int] = None) -> str:
        """
        Appends an event to the stream.
        
        Args:
             data (Dict): The payload.
             max_len (Optional[int]): Max stream length.
             
        Returns:
             str: The generated message ID.
        """
        client = self.connector.get_client()
        msg_id = await client.xadd(self.stream_key, data, maxlen=max_len)
        return msg_id

    async def claim_stuck_messages(self, min_idle_time_ms: int = 60000, count: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Auto-claims pending messages from crashed consumers.
        Using XAUTOCLAIM (Redis 6.2+ / Valkey) for efficiency.
        
        Args:
            min_idle_time_ms (int): Min idle time to consider a message 'stuck'.
            count (int): Max messages to claim.
            
        Returns:
             List[Tuple[str, Dict]]: Claimed messages.
        """
        client = self.connector.get_client()
        
        try:
            # xautoclaim(name, group, consumer, min_idle_time, start_id='0-0', count=None, justid=False)
            # Returns: (next_start_id, messages)
            # messages is a list of (msg_id, data)
            current_id = "0-0"
            all_messages = []
            
            # We might need to loop if we really want to grab 'count' messages, 
            # but XAUTOCLAIM returns a batch usually. We'll just take one batch for simplicity to avoid implementation complexity.
            next_id, messages = await client.xautoclaim(
                name=self.stream_key,
                groupname=self.group_name,
                consumername=self.consumer_name,
                min_idle_time=min_idle_time_ms,
                start_id=current_id,
                count=count
            )
            
            if messages:
                logger.warning(f"Consumer {self.consumer_name} claimed {len(messages)} stuck messages.")
                return messages
            return []
        except Exception as e:
            logger.error(f"Error during XAUTOCLAIM: {e}")
            return []
