# Examples Gallery

PSPF includes several working examples to help you understand different patterns and features. All examples are located in the `examples/` directory.

## Basic Valkey Demo
**File**: `examples/valkey_demo.py`  
**Description**: Shows a producer and consumer running in a single process using Valkey. Perfect for testing your local setup.
- Run: `python examples/valkey_demo.py`

## Stateful Word Count
**File**: `examples/stateful_wordcount.py`  
**Description**: Demonstrates stateful stream processing using the `SQLiteStateStore`. It counts occurrences of words in a simulated stream.
- Run: `python examples/stateful_wordcount.py`

## High Availability (HA) Demo
**File**: `examples/ha_demo.py`  
**Description**: Advanced demo showing leader election and log replication across multiple nodes.
- Run: `python examples/ha_demo.py`

## Memory Backend Verification
**File**: `examples/verify_memory.py`  
**Description**: A lightweight validation script that uses the `MemoryBackend` to test processor logic without external dependencies.
- Run: `python examples/verify_memory.py`

## Refactor Verification
**File**: `examples/verify_refactor.py`  
**Description**: End-to-end verification of the framework's core components (BatchProcessor, DLO, Metrics).
- Run: `python examples/verify_refactor.py`
