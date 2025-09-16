# Bedrock Transactions

**Distributed ACID transactions with optimistic concurrency control.**

Bedrock provides strict ACID guarantees using MVCC[^1] with per-transaction processes that enable read-your-writes consistency and sophisticated conflict detection.

## Key Features

### ACID Properties

- **Atomic**: All writes succeed or none do
- **Consistent**: All reads see same snapshot version
- **Isolated**: Serializable isolation through MVCC prevents interference  
- **Durable**: Universal log acknowledgment ensures persistence

### Performance Optimizations

- **Lazy versioning**: No network traffic until first read
- **Version leasing**: Read versions have expiration to prevent indefinite holds
- **Local caching**: Immediate read-your-writes visibility
- **Batching**: Multiple transactions processed together with intra-batch conflict detection
- **Horse racing**: Parallel queries to storage replicas
- **Binary format**: Efficient Transaction encoding with tagged sections and CRC validation

## Basic Usage

```elixir
# Basic transaction
Repo.transact(fn ->
  value = Repo.fetch(key)      # Read at consistent snapshot
  Repo.put(key, new_value)     # Write locally, commit later
  {:ok, :ok}
end)

# Read-only snapshot (faster, no commit phase)
Repo.snapshot(fn ->
  Repo.fetch(key)
end)
```

## Nested Transaction Usage

```elixir
Repo.transact(fn ->
  Repo.put(:outer, "value")

  Repo.transact(fn ->
    Repo.fetch(:outer)  # Sees parent writes
    Repo.put(:inner, "nested")
    {:ok, :ok}
  end)  # Local merge, no network traffic
  {:ok, :ok}
end)  # Only this commits to distributed system
```

## Transaction Flow

1. **Start**: Gateway creates dedicated Transaction Builder process[^2]
2. **Read**: First read gets consistent version from Sequencer[^3]  
3. **Write**: Changes accumulate locally (immediate visibility within transaction)
4. **Commit**: Distributed coordination with validation, conflict resolution, and durability
   - **Validation**: Transaction format and conflict summary validation
   - **Timeout handling**: Version ordering with configurable timeout (30s default)
   - **Conflict detection**: MVCC-based read/write conflict resolution
5. **Complete**: Client gets result, transaction process terminates

> **Complete Flow**: For detailed sequence diagrams and component interactions, see **[Transaction Processing Deep Dive](../deep-dives/transactions.md)**.

## Components

- **[Transaction Builder](../deep-dives/architecture/infrastructure/transaction-builder.md)**: Per-transaction process managing state and coordination
- **[Sequencer](../deep-dives/architecture/data-plane/sequencer.md)**: Global version authority implementing Lamport clock
- **[Commit Proxy](../deep-dives/architecture/data-plane/commit-proxy.md)**: Transaction batching and orchestration  
- **[Resolver](../deep-dives/architecture/data-plane/resolver.md)**: MVCC conflict detection using interval trees
- **[Log Servers](../deep-dives/architecture/data-plane/log.md)**: Durable storage with universal acknowledgment

> **Binary Format**: Transactions use Transaction encoding with tagged binary sections for efficient processing. See the [deep dive](../deep-dives/transactions.md#bedrocktransaction-format) for technical details.

[^1]: Multi-Version Concurrency Control - see [Glossary](../glossary.md#multi-version-concurrency-control)  
[^2]: [Transaction Builder](../deep-dives/architecture/infrastructure/transaction-builder.md)  
[^3]: [Sequencer](../deep-dives/architecture/data-plane/sequencer.md)  
