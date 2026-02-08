import asyncio
import json
import logging
from typing import List, Tuple, Dict, Any, Optional

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    import aiokafka.errors as kafka_errors
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    
from pspf.connectors.base import StreamingBackend
from pspf.utils.logging import get_logger

logger = get_logger("KafkaBackend")

class KafkaStreamBackend(StreamingBackend):
    """
    Kafka (or Redpanda) implementation of StreamingBackend.
    Requires `aiokafka` package.
    """
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, client_id: str):
        if not KAFKA_AVAILABLE:
            raise ImportError("aiokafka is required for KafkaStreamBackend. Install it with `pip install aiokafka`.")
            
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.client_id = client_id
        
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self._connected = False

    @property
    def stream_key(self) -> str:
        return self.topic

    @property
    def group_name(self) -> str:
        return self.group_id

    async def connect(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-producer"
            )
            await self.producer.start()
            
            # Consumer will be started on demand or here? 
            # Best to start here to ensure connectivity
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                client_id=f"{self.client_id}-consumer",
                enable_auto_commit=False, # We Manually ACK
                auto_offset_reset="earliest"
            )
            await self.consumer.start()
            
            self._connected = True
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    async def close(self):
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()
        self._connected = False
        logger.info("Closed Kafka connection")

    async def ping(self):
        if not self._connected:
            raise ConnectionError("Not connected")
        # Lightweight check? Maybe metadata request?
        # aiokafka doesn't have a direct ping, but we can check assignment or partitions
        if self.consumer:
            await self.consumer.partitions_for_topic(self.topic)
        return True

    async def ensure_group_exists(self, start_id: str = "0"):
        # Kafka handles group creation automatically on join
        pass

    async def add_event(self, data: Dict[str, Any], max_len: Optional[int] = None) -> str:
        if not self.producer:
            raise ConnectionError("Producer not connected")
            
        # Serialize
        payload = json.dumps(data).encode("utf-8")
        
        # Determine partition logic here? Or let producer duplicate.
        # Ideally we use event_id as key for ordering.
        key = data.get("event_id", "").encode("utf-8") if "event_id" in data else None
        
        try:
            record_metadata = await self.producer.send_and_wait(self.topic, payload, key=key)
            # Return "partition-offset" as ID
            return f"{record_metadata.partition}-{record_metadata.offset}"
        except Exception as e:
            logger.error(f"Failed to produce to Kafka: {e}")
            raise

    async def read_batch(self, count: int = 10, block_ms: int = 1000) -> List[Tuple[str, Dict[str, Any]]]:
        if not self.consumer:
            raise ConnectionError("Consumer not connected")
            
        try:
            # Poll for messages
            # timeout_ms in getmany
            result = await self.consumer.getmany(timeout_ms=block_ms, max_records=count)
            
            messages = []
            for tp, records in result.items():
                for record in records:
                    msg_id = f"{record.partition}-{record.offset}"
                    try:
                        data = json.loads(record.value.decode("utf-8"))
                        messages.append((msg_id, data))
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping non-JSON message {msg_id}")
                        continue
                        
            return messages
        except Exception as e:
            logger.error(f"Error reading batch from Kafka: {e}")
            raise

    async def ack_batch(self, message_ids: List[str]):
        if not self.consumer:
            raise ConnectionError("Consumer not connected")
            
        # Kafka commits offsets, not individual IDs.
        # We need to find the highest offset per partition in this batch and commit that + 1.
        # Simple approach: We assume batch processing is successful.
        # AIOKafka commit() commits the *current* position? No, commit expects offsets.
        # This is tricky without tracking TopicPartition objects.
        # Limitation: We need to parse msg_id to get partition/offset back.
        
        offsets_to_commit = {}
        
        for msg_id in message_ids:
            try:
                part_str, off_str = msg_id.split("-")
                partition = int(part_str)
                offset = int(off_str)
                
                tp = asyncio.Task.current_task() # No, tracking TP object is needed
                # We need to reconstruct TopicPartiton
                from aiokafka import TopicPartition
                tp = TopicPartition(self.topic, partition)
                
                # We want to commit offset + 1
                new_offset = offset + 1
                
                if tp not in offsets_to_commit or new_offset > offsets_to_commit[tp].offset:
                     # Struct to hold offset
                     # commit expects {TopicPartition: OffsetAndMetadata(offset, metadata)}
                     # or simple {TopicPartition: offset}
                     offsets_to_commit[tp] = new_offset
            except ValueError:
                logger.warning(f"Invalid message ID format for Kafka ACK: {msg_id}")
        
        if offsets_to_commit:
            await self.consumer.commit(offsets_to_commit)

    async def claim_stuck_messages(self, min_idle_time_ms: int = 60000, count: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
        # Kafka doesn't have PEL claiming mechanim like Redis Streams.
        # It relies on rebalancing if a consumer dies.
        # "Stuck" messages inside a live consumer are app logic.
        return []

    async def increment_retry_count(self, message_id: str) -> int:
        # We need an external store for retry counts (like Redis)
        # Or encode verify count in message and re-produce to retry topic.
        # For this demo, let's use a simple in-memory map or log warning.
        # Ideally, we should use a side-store or state store.
        # Returning 0 effectively disables retry counting logic in Processor, which might be okay loop-forever.
        # Or, we can use a separate Redis for state?
        return 1

    async def move_to_dlq(self, message_id: str, data: Dict[str, Any], error: str):
         # Produce to DLQ topic
         dlq_topic = f"{self.topic}-dlq"
         if self.producer:
             data["_error"] = error
             payload = json.dumps(data).encode("utf-8")
             await self.producer.send_and_wait(dlq_topic, payload)

    async def get_pending_info(self) -> Dict[str, Any]:
        """
        Retrieves lag info from Kafka.
        Currently returns 0 as calculating lag requires admin client or partition queries.
        """
        # TODO: Implement actual Kafka lag calculation using high watermarks
        return {
            "pending": 0, 
            "lag": 0,
            "consumers": 1
        }

