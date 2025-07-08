# Transaction Quick Reference

Essential transaction flow concepts for Bedrock development.

## Transaction Lifecycle (5 Phases)

1. **Initiation**: Client gets read version from Gateway → Sequencer
2. **Read Phase**: Client reads data at read version via Gateway → Storage
3. **Write Phase**: Client accumulates writes locally (no network calls)
4. **Commit Phase**: Gateway → Commit Proxy → Resolver → Log → Storage
5. **Completion**: Client receives success/failure notification

## Key Components

- **Gateway**: Client interface, manages read versions and transaction coordination
- **Sequencer**: Issues read versions and commit versions
- **Commit Proxy**: Batches transactions for conflict resolution
- **Resolver**: Detects conflicts using MVCC
- **Log**: Durable transaction log storage
- **Storage**: Manages key-value data and version history

## Critical Concepts

- **Read Version**: Consistent snapshot timestamp for all reads
- **Commit Version**: Final transaction commit timestamp
- **Conflict Detection**: Write-write conflicts checked at commit time
- **MVCC**: Multi-version concurrency control enables optimistic transactions

## Common Patterns

```elixir
# Basic transaction
Repo.transaction(fn repo ->
  value = Repo.fetch(repo, key)
  Repo.put(repo, key, new_value)
end)

# Read-only transaction (no commit phase)
Repo.snapshot(fn repo ->
  Repo.fetch(repo, key)
end)
```

## Testing Focus

- **Unit**: Test individual component behavior
- **Integration**: Test component interactions
- **End-to-End**: Test complete transaction flows

## See Also

- **Architecture**: [Architecture Guide](../01-guides/architecture-guide.md)
- **Implementation**: [Implementation Guide](../01-guides/implementation-guide.md)
- **Deep Dive**: [Architecture Deep](../02-deep/architecture-deep.md)