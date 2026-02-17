from dataclasses import dataclass
from typing import Optional, Any
from pspf.state.store import StateStore

@dataclass
class Context:
    """
    Context passed to processing functions.
    Contains access to state, metrics, and other utilities.
    """
    state: Optional[StateStore] = None
    # Future: 
    # topic: str
    # partition: int
    # timer_service: TimerService
