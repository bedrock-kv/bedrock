# Bedrock Transactions

**Distributed ACID transactions with optimistic concurrency control.**

Bedrock provides strict ACID guarantees using MVCC[^1] with per-transaction processes that enable read-your-writes consistency and sophisticated conflict detection.

## How Transactions Are Used

```elixir
# Basic transaction
Repo.transaction(fn repo ->
  value = Repo.fetch(repo, key)      # Read at consistent snapshot
  Repo.put(repo, key, new_value)     # Write locally, commit later
end)

# Read-only snapshot (faster, no commit phase)
Repo.snapshot(fn repo ->
  Repo.fetch(repo, key)
end)
```

## Transaction Flow

1. **Start**: Gateway creates dedicated Transaction Builder process[^2]
2. **Read**: First read gets consistent version from Sequencer[^3]
3. **Write**: Changes accumulate locally (immediate visibility within transaction)
4. **Commit**: Distributed coordination checks conflicts[^4] and ensures durability[^5]
5. **Complete**: Client gets result, transaction process terminates

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

### Nested Transactions

```elixir
Repo.transaction(fn repo ->
  Repo.put(repo, :outer, "value")
  
  Repo.transaction(fn inner ->
    Repo.fetch(inner, :outer)  # Sees parent writes
    Repo.put(inner, :inner, "nested")
  end)  # Local merge, no network traffic
end)  # Only this commits to distributed system
```

## Conflict Handling

Optimistic concurrency with immediate abort notification:

```elixir
case Repo.transaction(fn repo -> operations end) do
  {:ok, result} -> result
  {:error, :aborted} -> retry_with_backoff()  # Natural result
end
```

**Conflict types**: Read-write, write-write, and intra-batch conflicts detected by Resolvers using interval trees[^6].

## Components

- **Transaction Builder**: Per-transaction process in Gateway layer managing state
- **Sequencer**: Global version authority (Lamport clock)
- **Commit Proxy**: Batching and orchestration  
- **Resolver**: MVCC conflict detection
- **Log Servers**: Durable storage with universal acknowledgment

---

**Footnotes:**  
[^1]: Multi-Version Concurrency Control - see [Glossary](../glossary.md#multi-version-concurrency-control)  
[^2]: [Transaction Builder Component](../components/infrastructure/transaction-builder.md)  
[^3]: [Sequencer Component](../components/data-plane/sequencer.md)  
[^4]: [Conflict Resolution](../deep-dives/transactions.md)  
[^5]: [Transaction Processing Deep Dive](../deep-dives/transactions.md)  
[^6]: [Resolver and Interval Trees](../components/data-plane/resolver.md)  
