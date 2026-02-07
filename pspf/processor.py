import asyncio
import signal
import time
from typing import Callable, Awaitable, Dict, Any, List, Optional
from pspf.utils.logging import get_logger
from pspf.connectors.valkey import ValkeyStreamBackend
from pspf.telemetry import TelemetryManager
from opentelemetry import trace

logger = get_logger("BatchProcessor")

class BatchProcessor:
    """
    Handles the reliable processing loop for streams.

    Features:
    - Batch processing (XREADGROUP)
    - Graceful Shutdown (SIGTERM/SIGINT)
    - Dead Letter Office (DLO) routing for failed messages
    - Worker Recovery (XAUTOCLAIM)

    Attributes:
        backend (ValkeyStreamBackend): The backend to consume from.
        max_retries (int): Max attempts before moving to DLO.
    """
    def __init__(self, backend: ValkeyStreamBackend, max_retries: int = 3):
        """
        Initialize the BatchProcessor.

        Args:
            backend (ValkeyStreamBackend): The backend instance.
            max_retries (int): Number of retries before DLO. Default 3.
        """
        self.backend = backend
        self.max_retries = max_retries
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._shutdown_complete = asyncio.Event()
        self.telemetry = TelemetryManager()
        self.tracer = self.telemetry.get_tracer()

    def _setup_signals(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            except NotImplementedError:
                # Windows support or special environments
                pass
    
    async def shutdown(self):
        """
        Initiate graceful shutdown.

        Stops the consumption loop and waits for the current batch to finish 
        processing.
        """
        if self._running:
            logger.info("Shutdown signal received. Finishing current batch...")
            self._running = False
            self._shutdown_event.set()
            # Wait for cleanup
            try:
                await asyncio.wait_for(self._shutdown_complete.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Shutdown timed out, forcing exit.")

    async def run_loop(self, 
                       handler: Callable[[str, Dict[str, Any]], Awaitable[None]], 
                       batch_size: int = 10, 
                       poll_interval: float = 0.1):
        """
        Execute the main consume-process-ack loop.

        Args:
            handler (Callable): Async function to process each message. 
                                Signature: (msg_id, data) -> Awaitable[None]
            batch_size (int): Max items to read per cycle.
            poll_interval (float): Seconds to sleep if no messages found.
        """
        self._running = True
        self._setup_signals()
        
        stream_name = self.backend.stream_key

        # initial recovery check
        await self._recover_stuck_messages(handler)

        logger.info(f"Entered run loop. Consuming from {self.backend.stream_key} (Group: {self.backend.group_name})")

        while self._running:
            try:
                # 1. Read Batch
                READ_START = time.time()
                messages = await self.backend.read_batch(count=batch_size, block_ms=2000)
                
                # Update lag metric occasionally (mocked or real implementation needed)
                # self.telemetry.metrics.active_consumers.labels(stream=stream_name, group=self.backend.group_name).set(1)

                if not messages:
                    # Check shutdown flag again before sleeping
                    if not self._running:
                        break
                    await asyncio.sleep(poll_interval)
                    continue

                # 2. Process Batch
                processed_ids = []
                for msg_id, data in messages:
                    PROCESS_START = time.time()
                    
                    # Extract Context
                    ctx = self.telemetry.extract_context(data)
                    
                    # Start Span
                    with self.tracer.start_as_current_span(
                        "process_message", 
                        context=ctx,
                        attributes={"messaging.message_id": msg_id, "messaging.destination": stream_name}
                    ) as span:
                        try:
                            await handler(msg_id, data)
                            processed_ids.append(msg_id)
                            
                            # Metrics
                            duration = time.time() - PROCESS_START
                            self.telemetry.metrics.messages_processed.labels(stream=stream_name, status="success").inc()
                            self.telemetry.metrics.processing_latency.labels(stream=stream_name).observe(duration)
                            
                        except Exception as e:
                            duration = time.time() - PROCESS_START
                            self.telemetry.metrics.messages_processed.labels(stream=stream_name, status="error").inc()
                            
                            logger.error(f"Error processing message {msg_id}: {e}")
                            span.record_exception(e)
                            span.set_status(trace.Status(trace.StatusCode.ERROR))
                            
                            await self._handle_processing_error(msg_id, data, e)

                # 3. ACK Batch
                if processed_ids:
                    await self.backend.ack_batch(processed_ids)

            except asyncio.CancelledError:
                logger.info("Loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Unexpected error in run_loop: {e}")
                await asyncio.sleep(1.0) # Backoff

        self._shutdown_complete.set()
        logger.info("Processor stopped gracefully.")

    async def _handle_processing_error(self, msg_id: str, data: Dict[str, Any], error: Exception):
        """
        Handle processing failures with Retry and DLO logic.

        Increments the retry count in Redis. If max_retries is exceeded, 
        moves message to DLQ and ACKs it in the main group.

        Args:
            msg_id (str): The ID of the failed message.
            data (Dict): The message payload.
            error (Exception): The exception that caused the failure.
        """
        try:
            count = await self.backend.increment_retry_count(msg_id)
            if count > self.max_retries:
                logger.error(f"Message {msg_id} exceeded max retries ({self.max_retries}). Moving to DLO.")
                await self.backend.move_to_dlq(msg_id, data, str(error))
                # Metric for DLO?
                self.telemetry.metrics.messages_processed.labels(stream=self.backend.stream_key, status="dead_letter").inc()
            else:
                logger.info(f"Message {msg_id} failed {count}/{self.max_retries} times. Leaving in PEL for retry.")
        except Exception as inner_e:
            logger.critical(f"Failed to handle error for message {msg_id}: {inner_e}")

    async def _recover_stuck_messages(self, handler: Callable[[str, Dict[str, Any]], Awaitable[None]]):
        """
        Recover messages claimed by crashed workers.

        Uses XAUTOCLAIM to find messages idle for > 60s and re-processes them.

        Args:
            handler (Callable): The processing function.
        """
        try:
            # Try to claim messages that have been idle > 60s
            messages = await self.backend.claim_stuck_messages(min_idle_time_ms=60000, count=50)
            if messages:
                logger.info(f"Recovered {len(messages)} pending messages.")
                processed_ids = []
                for msg_id, data in messages:
                    try:
                        await handler(msg_id, data)
                        processed_ids.append(msg_id)
                    except Exception as e:
                        logger.error(f"Failed to process recovered message {msg_id}: {e}")
                        await self._handle_processing_error(msg_id, data, e)
                
                if processed_ids:
                    await self.backend.ack_batch(processed_ids)
        except Exception as e:
            logger.error(f"Error during recovery: {e}")
