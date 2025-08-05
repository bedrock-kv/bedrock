# Bedrock Transactions

**Distributed ACID transactions with optimistic concurrency control.**

Bedrock provides strict ACID guarantees using MVCC¹ with per-transaction processes that enable read-your-writes consistency and sophisticated conflict detection.

## How Transactions Work

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

1. **Start**: Gateway creates dedicated Transaction Builder process²
2. **Read**: First read gets consistent version from Sequencer³
3. **Write**: Changes accumulate locally (immediate visibility within transaction)
4. **Commit**: Distributed coordination checks conflicts⁴ and ensures durability⁵
5. **Complete**: Client gets result, transaction process terminates

## Key Features

### ACID Properties

- **Atomic**: All writes succeed or none do
- **Consistent**: All reads see same snapshot version
- **Isolated**: Strict serialization prevents interference  
- **Durable**: Universal log acknowledgment ensures persistence

### Performance Optimizations

- **Lazy versioning**: No network traffic until first read
- **Local caching**: Immediate read-your-writes visibility
- **Batching**: Multiple transactions processed together
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

**Conflict types**: Read-write, write-write, and intra-batch conflicts detected by Resolvers using interval trees⁶.

## Components

- **Transaction Builder**: Per-transaction process managing state
- **Sequencer**: Global version authority (Lamport clock)
- **Commit Proxy**: Batching and orchestration  
- **Resolver**: MVCC conflict detection
- **Log Servers**: Durable storage with universal acknowledgment

---

**Footnotes:**  
² [Transaction Builder Component](../components/control-plane/transaction-builder.md)  
⁵ [Transaction Processing Deep Dive](../deep-dives/transactions.md)  
