from typing import Any, Dict, Optional, Type, Generic, TypeVar, Callable, Awaitable
from pspf.connectors.base import StreamingBackend
from pspf.schema import BaseEvent, SchemaRegistry
from pspf.processor import BatchProcessor
from pspf.settings import settings
from pspf.telemetry import TelemetryManager
from pydantic import BaseModel
from pspf.utils.logging import get_logger
from pspf.state.store import StateStore
from pspf.processing.windows import Window

logger = get_logger("Stream")

T = TypeVar("T", bound=BaseModel)

class Stream(Generic[T]):
    """
    High-level Facade for Stream Processing using Composition.

    Manages the lifecycle of a stream processor, including connection handling,
    schema validation, and the processing loop.

    Attributes:
        backend (StreamingBackend): The storage backend for stream operations.
        schema (Optional[Type[T]]): Pydantic model for data validation.
        state_store (Optional[StateStore]): Optional store for stateful aggregations.
        processor (BatchProcessor): Internal processor for batch handling.
        telemetry (TelemetryManager): Observability manager.
    """
    def __init__(self, 
                 backend: StreamingBackend,
                 schema: Optional[Type[T]] = None,
                 state_store: Optional[StateStore] = None):
        """
        Initialize the Stream facade.

        Args:
            backend (ValkeyStreamBackend): Configured backend instance.
            schema (Optional[Type[T]]): Pydantic model class for validation. 
                                        If None, uses dynamic SchemaRegistry.
            state_store (Optional[StateStore]): Key-Value store for persistent state.
        """
        self.backend = backend
        self.schema = schema
        self.state_store = state_store
        self.processor = BatchProcessor(backend)
        self.telemetry = TelemetryManager()
        self._subscriptions: List[Dict[str, Any]] = []
        self._processors: List[BatchProcessor] = []
        
    def subscribe(self, topic: str, batch_size: int = 10) -> Callable:
        """Decorator to register a stateless handler for a topic."""
        def decorator(func: Callable) -> Callable:
            self._subscriptions.append({
                "type": "subscribe",
                "topic": topic,
                "handler": func,
                "batch_size": batch_size
            })
            return func
        return decorator

    def window(self, topic: str, window: Window, batch_size: int = 10, watermark_delay_ms: int = 0) -> Callable:
        """Decorator to register a stateful aggregation handler for a topic."""
        def decorator(func: Callable) -> Callable:
            self._subscriptions.append({
                "type": "aggregate",
                "topic": topic,
                "window": window,
                "handler": func,
                "batch_size": batch_size,
                "watermark_delay_ms": watermark_delay_ms
            })
            return func
        return decorator

    async def run_forever(self) -> None:
        """Start the infinite processing loop for all registered decorators concurrently."""
        import asyncio
        if not self._subscriptions:
            logger.warning("No subscriptions registered. Exiting run_forever.")
            return

        await self.backend.connect()
        if self.state_store:
            await self.state_store.start()

        tasks = []
        self._processors = []
        for i, sub in enumerate(self._subscriptions):
            topic = sub["topic"]
            backend_clone = self.backend.clone_with_topic(topic)
            await backend_clone.ensure_group_exists()
            
            processor = BatchProcessor(backend_clone, state_store=self.state_store, start_admin_server=(i == 0))
            self._processors.append(processor)
            
            if sub["type"] == "subscribe":
                typed_handler = self._create_typed_handler(sub["handler"], topic)
                task = asyncio.create_task(processor.run_loop(typed_handler, batch_size=sub["batch_size"]))
                tasks.append(task)
            elif sub["type"] == "aggregate":
                if not self.state_store:
                    raise RuntimeError(f"A StateStore must be provided to use @stream.window on {topic}")
                typed_agg_handler = self._create_aggregation_handler(sub["handler"], topic, sub["window"], sub["watermark_delay_ms"], backend_clone)
                task = asyncio.create_task(processor.run_loop(typed_agg_handler, batch_size=sub["batch_size"]))
                tasks.append(task)
                
        logger.info(f"Starting {len(tasks)} Stream processors concurrently.")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("run_forever cancelled.")
        finally:
            for p in self._processors:
                await p.shutdown()
            if self.state_store:
                await self.state_store.stop()

    async def stop(self) -> None:
        """Stop all running processors."""
        for p in self._processors:
            await p.shutdown()
        logger.info("Stream stopped.")
        
    async def health(self) -> Dict[str, str]:
        """
        Perform a health check on the stream components.

        Pings the backend to ensure connectivity.

        Returns:
            Dict[str, str]: A dictionary containing status ('ok' or 'error') 
                            and details.
        """
        try:
            # Assuming backend has a ping method (we will add it)
            await self.backend.ping()
            # Could also check if group exists
            return {"status": "ok", "backend": "connected"}
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"status": "error", "reason": str(e)}

    async def __aenter__(self) -> "Stream[T]":
        """
        Async context manager entry.
        
        Connects to the backend and ensures the consumer group exists.
        """
        # We delegate connection management to the backend's connector 
        # if the user hasn't already connected it.
        # But logically, the backend should be ready. 
        # We will ensure the group exists here.
        await self.backend.connect() # Idempotent-ish check inside
        await self.backend.ensure_group_exists()
        
        if self.state_store:
            await self.state_store.start()
            
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """
        Async context manager exit.
        
        Trigger shutdown of the processor and closes the backend connection.
        """
        if self.processor:
            await self.processor.shutdown()
            
        if self.state_store:
            await self.state_store.stop()
            
        # We close the connection here as we "took ownership" in aenter
        await self.backend.close()

    async def emit(self, event: Any, topic: Optional[str] = None) -> str:
        """
        Produce an event to the stream.

        Accepts either a Pydantic model or a dictionary.
        Serializes and injects tracing context before sending to the backend.

        Args:
            event (Any): The event object to publish (BaseModel or dict).
            topic (Optional[str]): The stream/topic to publish to. Defaults to backend's stream_key.

        Returns:
            str: The message ID generated by the backend.
        """
        if hasattr(event, "model_dump"):
            data = event.model_dump(mode='json')
            if "event_type" not in data:
                data["event_type"] = event.__class__.__name__
        elif isinstance(event, dict):
            data = event.copy()
            if "event_type" not in data:
                data["event_type"] = "GenericEvent"
        else:
            raise ValueError("Event must be a Pydantic model or a dictionary")
        # Ensure event_type is present for deserialization
        if "event_type" not in data:
            data["event_type"] = event.__class__.__name__

        # Inject Trace Context
        # We add a hidden field to carry the trace context
        self.telemetry.inject_context(data)
            
        target_backend = self.backend
        if topic and topic != self.backend.stream_key:
            target_backend = self.backend.clone_with_topic(topic)
            # Depending on backend, might need to ensure connection. For Valkey/Memory it shares the connector.
            
        msg_id = await target_backend.add_event(data)
        return msg_id

    async def run(self, handler: Callable[[T], Awaitable[None]], batch_size: int = 10) -> None:
        """
        Start the infinite processing loop.

        Continuously fetches batches of messages, validates them against the schema,
        and invokes the user-provided handler.

        Args:
            handler (Callable[[T], Awaitable[None]]): Async function to process each event.
            batch_size (int): Number of messages to fetch in each batch. Default is 10.

        Raises:
             Exception: Any unhandled exception from the processor or validation logic.
        """
        async def typed_handler(msg_id: str, raw_data: Dict[str, Any]) -> None:
            return await self._create_typed_handler(handler, self.backend.stream_key)(msg_id, raw_data)

        logger.info(f"Starting stream processor for {self.backend.stream_key}...")
        await self.processor.run_loop(typed_handler, batch_size=batch_size)

    def _create_typed_handler(self, handler: Callable, topic: str) -> Callable[[str, Dict[str, Any]], Awaitable[None]]:
        async def typed_handler(msg_id: str, raw_data: Dict[str, Any]) -> None:
            # 1. Deserialize / Validate
            if self.schema:
                try:
                    event = self.schema.model_validate(raw_data)
                except Exception as e:
                    logger.error(f"Schema validation failed for msg {msg_id}: {e}")
                    # Re-raise so processor handles DLO logic
                    raise e
            else:
                # Dynamic validation via Registry or fallback
                # Note: raw_data might need cleanup if it has Redis artifacts? 
                # (Valkey decode_responses=True handles bytes->str)
                try:
                    # cast is needed because validate returns BaseModel but we expect T
                    from typing import cast
                    event = cast(T, SchemaRegistry.validate(raw_data))
                except Exception as e:
                    logger.error(f"Dynamic validation failed for msg {msg_id}: {e}")
                    raise e
                
            # Inject metadata
            if isinstance(event, BaseEvent):
                event.offset = msg_id
            
            # 2. Call User Logic
            # We explicitly cast event to T (or close enough)
            await handler(event) # type: ignore
        return typed_handler

    async def aggregate(self, 
                        window: Window, 
                        handler: Callable[[T, Any], Awaitable[Any]],
                        batch_size: int = 10,
                        watermark_delay_ms: int = 0) -> None:
        """
        Start the infinite processing loop with stateful windowed aggregation.

        Args:
            window (Window): The windowing strategy to apply.
            handler (Callable[[T, Any], Awaitable[Any]]): Async function taking (event, current_state) 
                                                          and returning the new state.
            batch_size (int): Number of messages to fetch in each batch. Default is 10.
            
        Raises:
            RuntimeError: If no StateStore is attached to the Stream.
        """
        if not self.state_store:
            raise RuntimeError("A StateStore must be provided to Stream.__init__ to use aggregate()")

        typed_agg_handler = self._create_aggregation_handler(handler, self.backend.stream_key, window, watermark_delay_ms, self.backend)
        logger.info(f"Starting windowed aggregation processor for {self.backend.stream_key}...")
        await self.processor.run_loop(typed_agg_handler, batch_size=batch_size)

    def _create_aggregation_handler(self, handler: Callable, topic: str, window: Window, watermark_delay_ms: int, backend: StreamingBackend) -> Callable[[str, Dict[str, Any]], Awaitable[None]]:
        max_event_ts = 0.0

        async def aggregation_handler(msg_id: str, raw_data: Dict[str, Any]) -> None:
            nonlocal max_event_ts
            # 1. Deserialize / Validate (similar to run_loop)
            if self.schema:
                try:
                    event = self.schema.model_validate(raw_data)
                except Exception as e:
                    logger.error(f"Schema validation failed for msg {msg_id}: {e}")
                    raise e
            else:
                try:
                    from typing import cast
                    event = cast(T, SchemaRegistry.validate(raw_data))
                except Exception as e:
                    logger.error(f"Dynamic validation failed for msg {msg_id}: {e}")
                    raise e
                    
            if isinstance(event, BaseEvent):
                event.offset = msg_id

            # 2. Extract Timestamp
            # Prioritize event timestamp if available, else fallback to current time
            # For robustness, we should use the actual event time
            ts = getattr(event, "timestamp", None)
            if ts is None:
                # If no timestamp field, we inject one or fail?
                # Fallback to current time
                import time
                ts_val = time.time()
            else:
                from datetime import datetime
                if isinstance(ts, datetime):
                    ts_val = ts.timestamp()
                elif isinstance(ts, (int, float)):
                    ts_val = float(ts)
                else:
                    import time
                    ts_val = time.time()

            # 3. Handle Watermarks
            max_event_ts = max(max_event_ts, ts_val)
            current_watermark = max_event_ts - (watermark_delay_ms / 1000.0)

            # 4. Assign Windows
            windows = window.assign_windows(ts_val)
            
            # 5. Process Each Window Location
            # Key extraction: prefer 'key' attribute, else default
            event_key = getattr(event, "key", "default_key")
            
            for start, end in windows:
                if watermark_delay_ms > 0 and end < current_watermark:
                    logger.warning(f"Dropping late event {msg_id}. Window {end} is older than Watermark {current_watermark:.2f}")
                    continue
                
                # Dynamic Logic for Sessions vs Fixed Windows
                if window.is_session:
                    # Sessions are tracked by a moving active key
                    state_key = f"{self.backend.stream_key}:{event_key}:session:active"
                    current_raw = await self.state_store.get(state_key)
                    
                    # session_state structure: {"start": float, "last": float, "agg": Any}
                    if current_raw and isinstance(current_raw, dict) and "last" in current_raw:
                        last_activity = current_raw["last"]
                        # Check activity gap (Note: SessionWindow.gap_ms is in ms)
                        if (ts_val - last_activity) <= (window.gap_ms / 1000.0):
                            # Within gap: Update existing session
                            current_state = current_raw.get("agg")
                            new_agg = await handler(event, current_state)
                            new_state = {
                                "start": current_raw["start"],
                                "last": ts_val,
                                "agg": new_agg
                            }
                        else:
                            # Outside gap: Old session is effectively closed. 
                            # We could move it to a 'closed' archive key here if needed.
                            # For now, we just start a fresh one.
                            new_agg = await handler(event, None)
                            new_state = {
                                "start": ts_val,
                                "last": ts_val,
                                "agg": new_agg
                            }
                    else:
                        # Fresh session
                        new_agg = await handler(event, None)
                        new_state = {
                            "start": ts_val,
                            "last": ts_val,
                            "agg": new_agg
                        }
                else:
                    # Fixed Windows (Tumbling/Sliding)
                    state_key = f"{self.backend.stream_key}:{event_key}:{start}:{end}"
                    current_state = await self.state_store.get(state_key)
                    new_state = await handler(event, current_state) # type: ignore
                
                # Save state
                await self.state_store.put(state_key, new_state)
            
            # Atomically checkpoint offset for exactly-once-ish semantics
            # if the state store supports transactional combined checkpointing.
            # ( SQLiteStateStore does this )
            group_name = getattr(backend, "group_name", "default_group")
            await self.state_store.checkpoint(
                stream_id=topic,
                group_id=group_name,
                offset=msg_id
            )
            
        return aggregation_handler
