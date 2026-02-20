# Backends Overview

PSPF is backend-agnostic. You can switch message brokers by simply changing the `StreamingBackend` implementation injected into your `Stream`.

## Comparison Table

| Feature | Valkey (Recommended) | Kafka | File (Local Log) | Memory |
| :--- | :--- | :--- | :--- | :--- |
| **Persistence** | Permanent | Permanent | Permanent | None |
| **Consumer Groups** | Yes | Yes | No | Yes (Mocked) |
| **HA Support** | High | High | Low | None |
| **Complexity** | Low | High | Very Low | None |
| **Best For** | Production | Enterprise Scale | Local Dev / Edge | Testing |

## Valkey (ValkeyStreamBackend)
The primary production backend. It uses Redis-compatible streams to provide high-performance, ordered delivery with robust consumer group management.

## Kafka (KafkaStreamBackend)
Leverages `aiokafka` to integrate with existing Kafka or Redpanda clusters. Useful for bridging PSPF applications with broader enterprise data pipelines.

## File (LocalLog)
The "Native Log" implementation. It stores events on disk in a binary MessagePack format with CRC32 checksums.
> [!WARNING]
> The high-level `Stream` facade wrapper for `LocalLog` is currently in **preview**.

## Memory (MemoryBackend)
A volatile backend that stores everything in RAM. Ideal for unit tests and local verification without external dependencies.
