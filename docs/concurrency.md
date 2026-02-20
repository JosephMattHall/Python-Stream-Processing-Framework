# Partitioned Concurrency

PSPF uses **Partition-Based Scaling** to allow multiple consumers to process a single stream of data in parallel while maintaining strict order for related entities.

## Key-Based Partitioning

When a producer emits an event, it can provide a **Key** (e.g., `user_id` or `order_id`).
- Events with the **same key** are guaranteed to land in the **same partition**.
- Events within a single partition are always processed in the order they were written.

This ensures that for a specific entity, you never process "Update 2" before "Update 1", even with 100 workers running.

## Consumer Groups

To scale horizontally, you simply run multiple instances of your application with the same `group_name`.
- **Load Balancing**: Each worker in the group is assigned a subset of the available partitions.
- **Exclusive Access**: Only one worker in a group processes a specific partition at any given time.
- **Rebalancing**: If a worker joins or leaves, PSPF (via the backend) redistributes partitions among the remaining members.

## Partition Leases

In complex or high-availability setups, PSPF uses a **Partition Lease Manager**:
1. **Acquiring Leases**: Workers compete for leases on partitions in Valkey.
2. **Heartbeating**: Workers must periodically renew their leases.
3. **Stealing**: If a worker fails to heartbeat, its lease expires and another worker can "steal" it, ensuring processing continues.

> [!NOTE]
> Currently, the `ValkeyStreamBackend` relies on native Redis Consumer Group semantics for partition management.
