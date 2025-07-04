# Transaction Lifecycle in Bedrock

This document explains the complete lifecycle of a transaction in Bedrock, from initiation to commit.

## See Also
- **Architecture Overview**: [FoundationDB Concepts](foundationdb-concepts.md) - Core architectural principles and component relationships
- **Persistent State**: [Persistent Configuration](persistent-configuration.md) - How cluster state is maintained across restarts
- **Implementation Details**: [Data Plane Components](../03-implementation/data-plane-components.md) - Detailed implementation of transaction processing components
- **Development Support**: [Best Practices](../02-development/best-practices.md) and [Debugging Strategies](../02-development/debugging-strategies.md)
- **Complete Reference**: [Bedrock Architecture Livebook](../../docs/bedrock-architecture.livemd) - Comprehensive architectural overview

## Overview

Bedrock transactions follow a multi-phase lifecycle inspired by FoundationDB:

1. **Initiation**: Client obtains a read version
2. **Read Phase**: Client reads data at the read version
3. **Write Phase**: Client accumulates writes locally
4. **Commit Phase**: Transaction is submitted, resolved, and logged
5. **Completion**: Client is notified of success or failure

## Detailed Transaction Flow

### 1. Transaction Initiation

```elixir
# Client code
Repo.transaction(fn repo ->
  # Transaction operations will go here
end)

# Under the hood
{:ok, read_version} = Gateway.get_read_version()
Gateway.lease_read_version(read_version, timeout)
```

When a transaction begins:
- The client contacts the Gateway
- The Gateway requests a read version from the Sequencer
- This read version represents a consistent snapshot of the database
- The Gateway provides a lease for this read version
- All reads in the transaction will use this read version

### 2. Read Phase

```elixir
# Client code
value = Repo.fetch(repo, "customer/123")

# Under the hood
{:ok, value} = Gateway.fetch(read_version, "customer/123")
```

During the read phase:
- The client reads keys using the transaction's read version
- The Gateway tracks all keys and ranges read
- Storage servers provide data at the specified read version
- If a key has been written in the current transaction, the pending write value is returned
- All reads are consistent with the read version snapshot

### 3. Write Phase

```elixir
# Client code
Repo.put(repo, "customer/123/balance", 500)

# Under the hood
Gateway.put("customer/123/balance", 500)
```

During the write phase:
- Writes are accumulated locally in the Gateway
- No network traffic occurs for writes until commit
- The client can read its own writes within the transaction
- The Gateway tracks all keys written

### 4. Commit Phase

```elixir
# Client code (implicit at the end of the transaction block)
# The transaction block returns, triggering commit

# Under the hood
CommitProxy.commit({
  {read_version, read_keys_and_ranges},  # reads
  write_key_values                       # writes
})
```

The commit phase involves several steps:

#### 4.1 Commit Proxy Batching

- The transaction is sent to a Commit Proxy
- The Commit Proxy batches multiple transactions
- Batching continues for a few milliseconds or until batch size limit
- The Commit Proxy requests the next commit version from the Sequencer

#### 4.2 Conflict Resolution

- The batch is sent to Resolvers based on key ranges
- Resolvers check for conflicts:
  - Read-Write conflicts: Transaction read a key that was written by a later-committed transaction
  - Write-Write conflicts: Two transactions wrote to the same key
  - Within-batch conflicts: Transactions in the same batch conflict with each other
- Resolvers return a list of transactions that must be aborted

#### 4.3 Transaction Logging

- Non-conflicting transactions are combined into a single set of changes
- These changes are sent to Log servers for durable storage
- The Commit Proxy waits for acknowledgment from all relevant Log servers
- The commit is assigned the batch's commit version

### 5. Completion

```elixir
# Client code receives result
{:ok, result} = Repo.transaction(fn repo ->
  # Transaction operations
  result
end)

# Or for conflicts
{:error, :conflict} = Repo.transaction(fn repo ->
  # Transaction that had conflicts
end)
```

- Clients with aborted transactions receive a conflict error
- Clients with successful transactions receive success and their result
- The transaction's commit version is available for the client

## Version Management

Bedrock uses several version concepts:

- **Read Version**: The version at which a transaction reads data
- **Commit Version**: The version assigned when a transaction commits
- **Last Committed Version (LCV)**: The highest committed version in the system
- **Minimum Read Version (MRV)**: The oldest version still needed for active transactions

The system maintains a sliding window between MRV and LCV to efficiently manage MVCC.

## Transaction Guarantees

Bedrock transactions provide:

- **Atomicity**: All writes commit or none do
- **Consistency**: The database moves from one valid state to another
- **Isolation**: Strict serialization (transactions appear to execute sequentially)
- **Durability**: Committed transactions survive node failures

## Implementation Details

### Gateway Transaction State

The Gateway maintains transaction state including:
- Read version
- Read set (keys and ranges read)
- Write set (keys and values to write)
- Transaction options (timeouts, priorities, etc.)

### Conflict Window Management

Resolvers maintain a conflict window:
- Starts at the Minimum Read Version (MRV)
- Ends at the Last Committed Version (LCV)
- Contains information about which keys were read/written at each version
- Allows efficient conflict detection

### Storage Version Management

Storage servers:
- Maintain multiple versions of data
- Serve reads at specific versions
- Follow transaction logs to update their state
- Garbage collect versions older than the system-wide MRV

## Error Handling

### Transaction Conflicts

When conflicts occur:
- The client receives a conflict error
- The client can retry the transaction with a new read version
- Exponential backoff is recommended for retries

### Node Failures

- If a node fails during a transaction, the transaction may be aborted
- The Director detects node failures and reassigns services
- Recovery ensures system consistency after failures

## Performance Considerations

### Read Performance

- Reads are served directly from Storage servers
- Local reads (from Storage on the same node) are faster
- Read caching can improve performance for frequently accessed data

### Commit Performance

- Batching improves commit throughput
- Conflict resolution is the most CPU-intensive part of commit
- Log write performance determines commit latency

## Debugging Transactions

To debug transaction issues:

```elixir
# Enable transaction tracing (if implemented)
Logger.configure(level: :debug)

# Check transaction statistics
# This would depend on your telemetry setup
```

Common transaction issues:
- High conflict rates: Check for hot keys or poor transaction design
- Slow commits: Check Log server performance
- Read version delays: Check Sequencer performance
