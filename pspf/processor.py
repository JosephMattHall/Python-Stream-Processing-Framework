import asyncio
import signal
import time
from typing import Callable, Awaitable, Dict, Any, List, Optional
from pspf.utils.logging import get_logger
from pspf.connectors.base import StreamingBackend
from pspf.telemetry import TelemetryManager
from opentelemetry import trace
from pspf.settings import settings
from pspf.state.store import StateStore
from pspf.context import Context
import inspect

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
        backend (StreamingBackend): The backend to consume from.
        max_retries (int): Max attempts before moving to DLO.
        min_idle_time_ms (int): Min idle time for message recovery. Default 60000.
        state_store (StateStore): Optional state backend.
    """
    def __init__(self, backend: StreamingBackend, max_retries: int = 3, min_idle_time_ms: int = 60000, state_store: Optional[StateStore] = None):
        """
        Initialize the BatchProcessor.

        Args:
            backend (ValkeyStreamBackend): The backend instance.
            max_retries (int): Number of retries before DLO. Default 3.
        """
        self.backend = backend
        self.max_retries = max_retries
        self.min_idle_time_ms = min_idle_time_ms
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._shutdown_complete = asyncio.Event()
        self.telemetry = TelemetryManager()
        self.state_store = state_store
        self.tracer = self.telemetry.get_tracer()
        self._paused = False

    def pause(self):
        """Pause message consumption."""
        self._paused = True
        logger.info("Processor paused.")

    def resume(self):
        """Resume message consumption."""
        self._paused = False
        logger.info("Processor resumed.")

    def _setup_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            except NotImplementedError:
                # Windows support or special environments
                pass
    
    async def shutdown(self) -> None:
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
                       poll_interval: float = 0.1) -> None:
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

        if self.state_store:
            await self.state_store.start()

        logger.info(f"Entered run loop. Consuming from {self.backend.stream_key} (Group: {self.backend.group_name})")

        # Start Admin Server
        admin_task = asyncio.create_task(self._start_admin_server())
        
        # Start Lag Monitor
        monitor_task = asyncio.create_task(self._monitor_metrics(interval=10.0))
        
        # Set Worker Status = 1
        consumer_name = getattr(self.backend, 'consumer_name', 'unknown')
        self.telemetry.metrics.worker_status.labels(
            stream=stream_name, 
            group=self.backend.group_name,
            consumer=consumer_name
        ).set(1)

        try:
            while self._running:
                # Check Pause State
                if self._paused:
                     # Update status to 0 (Paused)
                     self.telemetry.metrics.worker_status.labels(
                        stream=stream_name, 
                        group=self.backend.group_name,
                        consumer=consumer_name
                    ).set(0)
                     await asyncio.sleep(1.0)
                     continue
                else:
                     # status 1 (Running)
                     self.telemetry.metrics.worker_status.labels(
                        stream=stream_name, 
                        group=self.backend.group_name,
                        consumer=consumer_name
                    ).set(1)

                try:
                    # 1. Read Batch
                    READ_START = time.time()
                    messages = await self.backend.read_batch(count=batch_size, block_ms=2000)
                    
                    if not messages:
                        # Check shutdown flag again before sleeping
                        if not self._running:
                            break
                        await asyncio.sleep(poll_interval)
                        continue

                    # 2. Process Batch
                    processed_ids = []
                    for msg_id, data in messages:
                        if await self._process_single_message(handler, msg_id, data, stream_name):
                            processed_ids.append(msg_id)

                    # 3. ACK Batch
                    if processed_ids:
                        await self.backend.ack_batch(processed_ids)

                except asyncio.CancelledError:
                    logger.info("Loop cancelled.")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error in run_loop: {e}")
                    await asyncio.sleep(1.0) # Backoff
        finally:
            monitor_task.cancel()
            
            # Shutdown Admin Server
            admin_task.cancel()
            
            self.telemetry.metrics.worker_status.labels(
                stream=stream_name, 
                group=self.backend.group_name,
                consumer=consumer_name
            ).set(0)
            try:
                await monitor_task
                await admin_task
            except asyncio.CancelledError:
                pass

        if self.state_store:
            await self.state_store.stop()

        self._shutdown_complete.set()
        logger.info("Processor stopped gracefully.")

    async def _start_admin_server(self):
        """
        Starts the Admin API server.
        """
        try:
            from uvicorn import Config, Server
            from pspf.admin import create_admin_app
            
            app = create_admin_app(self)
            config = Config(
                app=app, 
                host="0.0.0.0", 
                port=settings.ADMIN_PORT, 
                log_config=None,
                log_level="warning" 
            )
            server = Server(config)
            
            # Disable signal handlers as we manage them
            server.install_signal_handlers = lambda: None
            
            logger.info(f"Starting Admin API on port {settings.ADMIN_PORT}")
            await server.serve()
        except Exception as e:
            logger.error(f"Failed to start Admin API: {e}")

    async def _monitor_metrics(self, interval: float = 10.0):
        """
        Background task to update lag and other metrics.
        """
        while self._running:
            try:
                info = await self.backend.get_pending_info()
                lag = info.get("lag", 0)
                
                self.telemetry.metrics.lag.labels(
                    stream=self.backend.stream_key, 
                    group=self.backend.group_name
                ).set(lag)
                
                # We could also expose pending count if we added a metric for it
            except Exception as e:
                logger.warning(f"Error updating metrics: {e}")
            
            await asyncio.sleep(interval)

    async def _process_single_message(self, handler: Callable, msg_id: str, data: Dict[str, Any], stream_name: str) -> bool:
        """
        Process a single message with context injection, telemetry, and error handling.
        Returns True if successful, False otherwise.
        """
        PROCESS_START = time.time()
        ctx = self.telemetry.extract_context(data)
        
        with self.tracer.start_as_current_span(
            "process_message", 
            context=ctx,
            attributes={"messaging.message_id": msg_id, "messaging.destination": stream_name}
        ) as span:
            try:
                # Inspect handler signature to see if it wants context
                sig = inspect.signature(handler)
                if len(sig.parameters) >= 3:
                    # Stateful: handler(msg_id, data, ctx)
                    processing_ctx = Context(state=self.state_store)
                    await handler(msg_id, data, processing_ctx)
                else:
                    # Stateless: handler(msg_id, data)
                    await handler(msg_id, data)

                # Metrics
                duration = time.time() - PROCESS_START
                self.telemetry.metrics.messages_processed.labels(stream=stream_name, status="success").inc()
                self.telemetry.metrics.processing_latency.labels(stream=stream_name).observe(duration)
                return True

            except Exception as e:
                duration = time.time() - PROCESS_START
                self.telemetry.metrics.messages_processed.labels(stream=stream_name, status="error").inc()
                
                logger.error(f"Error processing message {msg_id}: {e}")
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR))
                
                await self._handle_processing_error(msg_id, data, e)
                return False

    async def _handle_processing_error(self, msg_id: str, data: Dict[str, Any], error: Exception) -> None:
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

    async def _recover_stuck_messages(self, handler: Callable[[str, Dict[str, Any]], Awaitable[None]]) -> None:
        """
        Recover messages claimed by crashed workers.

        Uses XAUTOCLAIM to find messages idle for > 60s and re-processes them.

        Args:
            handler (Callable): The processing function.
        """
        try:
            # Try to claim messages that have been idle > min_idle_time_ms
            messages = await self.backend.claim_stuck_messages(min_idle_time_ms=self.min_idle_time_ms, count=50)
            if messages:
                logger.info(f"Recovered {len(messages)} pending messages.")
                processed_ids = []
                for msg_id, data in messages:
                    if await self._process_single_message(handler, msg_id, data, self.backend.stream_key):
                        processed_ids.append(msg_id)
                
                if processed_ids:
                    await self.backend.ack_batch(processed_ids)
        except Exception as e:
            logger.error(f"Error during recovery: {e}")
