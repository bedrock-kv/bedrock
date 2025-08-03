# Vacancy Creation Phase

**Planning the new system architecture using placeholder abstractions.**

Recovery must plan for a complete new system configuration, but how do you specify what services you need without prematurely committing to specific machines or processes? The `VacancyCreationPhase` solves this planning problem by creating "vacancy" placeholders—numbered slots like `{:vacancy, 1}` that represent service requirements without binding them to actual services.

## The Vacancy Abstraction

Since Bedrock organizes data into shards (independent partitions) identified by "range tags," services with identical tag sets can substitute for each other, but services with different tag sets serve different data and cannot be interchanged. This shard structure drives the entire vacancy creation process: the phase must ensure each shard combination has enough services to meet fault tolerance requirements while deferring the specific assignment of services to shards until recruitment phases can see all available options.

This vacancy approach separates planning ("what do we need?") from assignment ("which services provide it?"), allowing recruitment phases to see the complete picture—all vacancies needing services and all services available to provide them—enabling optimizations like fault-tolerant placement across machines, minimizing data movement, and load balancing.

## Service Type Strategies

Recovery takes fundamentally different approaches for logs versus storage because they have different constraints:

### Log Vacancy Strategy

Logs contain no persistent state—they can be recreated by replaying transactions from other logs—so recovery uses a clean-slate approach, creating the desired number of new log vacancies for each shard combination from the old system. If the old system had logs serving `["shard_a"]` and `["shard_b"]`, and configuration specifies 3 logs per shard, recovery creates 6 log vacancies total, with old logs becoming candidates for filling these vacancies but not automatically preserved.

### Storage Vacancy Strategy

Storage servers, however, contain persistent data that's expensive to migrate, so recovery takes a conservative approach, preserving existing storage assignments and only creating additional vacancies where needed—if shard A has 2 storage servers but configuration requires 3, recovery creates just 1 storage vacancy for shard A.

### Resolver Vacancy Strategy

Resolvers, unlike logs and storage, are purely computational components that maintain transaction conflict detection state. The phase creates resolver descriptors that map each storage team's key range to a resolver vacancy placeholder. These descriptors define the boundaries between resolvers (where one resolver's responsibility ends and another's begins) without committing to specific resolver processes. Each resolver descriptor contains a start key from the storage teams and a vacancy mark, allowing resolver startup to later assign actual resolver processes that will handle MVCC conflict detection for their assigned key ranges.

## Result Specification

The result is a complete specification of the desired new system: log vacancies tagged by shard, storage teams with additional vacancy slots, resolver descriptors with vacancy marks, and a clear handoff to recruitment phases that will fill these positions with actual services.

## Input Parameters

- `context.old_transaction_system_layout.logs` - Used only to discover what shard combinations (tag sets) existed
- `context.old_transaction_system_layout.storage_teams` - List of storage team descriptors with tags and storage IDs  
- `context.cluster_config.parameters.desired_logs` - Target log count per tag set
- `context.cluster_config.parameters.desired_replication_factor` - Target replicas per storage tag set

## Output Results

- `recovery_attempt.logs` - Map of vacancy IDs to tag lists (original logs are replaced)
- `recovery_attempt.storage_teams` - Storage teams with expanded storage_ids including vacancies
- `recovery_attempt.resolvers` - Resolver descriptors mapping key ranges to resolver vacancies

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/vacancy_creation_phase.ex`

## Next Phase

Proceeds to [Version Determination](05-version-determination.md) to establish the recovery baseline for durable data.