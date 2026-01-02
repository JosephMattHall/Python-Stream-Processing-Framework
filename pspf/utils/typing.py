from typing import TypeVar, Any, Callable, Coroutine

T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")

# Function types
MapFunction = Callable[[T], U]
FilterFunction = Callable[[T], bool]
KeySelector = Callable[[T], Any]
ReduceFunction = Callable[[T, T], T]
