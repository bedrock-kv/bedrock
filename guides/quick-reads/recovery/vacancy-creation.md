# Vacancy Creation

**Planning new system architecture using abstract placeholders.**

The vacancy creation phase solves a key recovery challenge: how do you design a complete distributed system without immediately committing to specific machines? Recovery uses numbered vacancy slots like `{:vacancy, 1}` and `{:vacancy, 2}` as placeholders that represent service requirements without binding them to actual processes.

This placeholder approach enables global optimization during later [recruitment phases](log-recruitment.md), where the system can see all requirements and available services together to make optimal placement decisions.

## Core Strategy

Bedrock organizes data into [shards](../../glossary.md#shard)—independent partitions where services with identical tags can substitute for each other. The vacancy creation phase ensures each shard gets sufficient services for fault tolerance while maintaining placement flexibility.

Recovery uses different strategies for each service type based on their data preservation needs:

### Log Services: Clean Slate

[Log services](../../components/data-plane/log.md) contain no irreplaceable data—their [transaction](../../quick-reads/transactions.md) records exist identically in other logs. Recovery creates exactly the configured number of log vacancies for each shard, ignoring existing logs entirely.

Example: Previous system had logs for `["shard_a"]` and `["shard_b"]`. Configuration specifies 3 logs per shard. Recovery creates exactly 6 new log vacancies. Old logs become recruitment candidates but get no preservation guarantee.

### Storage Services: Preservation First

[Storage services](../../components/data-plane/storage.md) contain irreplaceable persistent data. Recovery preserves all existing storage assignments and creates additional vacancies only when configuration requires higher replication.

Example: Storage team has 2 replicas but configuration requires 3. Recovery creates exactly 1 storage vacancy for that team. No existing assignments change.

### Resolver Services: Computational Boundaries

[Resolvers](../../components/control-plane/resolver.md) handle [MVCC](../../glossary.md#multi-version-concurrency-control) conflict detection for key ranges. Recovery creates resolver descriptors that map each storage team's key range to resolver vacancy placeholders, establishing computational boundaries without assigning specific processes.

## Output Blueprint

The phase produces a complete architectural specification:

- **Log Vacancy Map**: Vacancy identifiers mapped to shard tag lists, completely replacing previous log architecture
- **Storage Team Expansion**: Existing storage teams with precisely calculated additional vacancy slots
- **Resolver Layout**: Key range to resolver vacancy mappings for complete keyspace coverage

## Next Phase

Vacancy creation feeds into [version determination](version-determination.md), which calculates the highest [version](../../glossary.md#version) guaranteed durable across the cluster.

---

**Implementation**: `lib/bedrock/control_plane/director/recovery/vacancy_creation_phase.ex`
