# Path Determination Phase

**Deciding between new cluster initialization or data recovery.**

Recovery splits into two paths based on locking results, though both converge on service recruitment. This phase determines whether the system requires initial configuration for a new cluster or data recovery from an existing cluster.

## New Cluster Initialization

When no logs existed in the previous layout, the system requires initial configuration. The `InitializationPhase` creates the initial transaction system layout for a new cluster, running only when no previous cluster state exists. It creates log vacancy placeholders for the desired number of logs and exactly two storage teams with fixed key range boundaries, using vacancy placeholders instead of assigning specific services immediately.

### Initial Configuration Details

Log vacancies are assigned initial version ranges `[0, 1]` to establish the starting point for transaction processing. Storage teams are created with a fundamental keyspace division:
- **User data team**: Handles keys from empty string to `0xFF`
- **System data team**: Handles keys from `0xFF` to end-of-keyspace

This division separates user keys from system keys, with keys above the user space reserved for system metadata and configuration. Creating vacancies rather than immediate service assignment allows later phases to optimize placement across available nodes and handle assignment failures independently. The phase always succeeds since it only creates in-memory structures.

### Input Parameters
- `context.cluster_config.parameters.desired_logs` - Number of log vacancies to create
- `context.cluster_config.parameters.desired_replication_factor` - Number of storage vacancies per team

### Output Results
- `durable_version` - Set to 0 for new cluster
- `old_log_ids_to_copy` - Empty list (no previous data)
- `version_vector` - Set to {0, 0} as starting point
- `logs` - Map of log vacancies with initial version ranges [0, 1]
- `storage_teams` - Two teams with fixed key range boundaries and vacancy placeholders

## Data Recovery Path

When logs existed in the previous layout, recovery proceeds to the data recovery path, which involves:
1. [Log Recovery Planning](03-log-recovery-planning.md) - Analyze what data can be safely recovered
2. [Vacancy Creation](04-vacancy-creation.md) - Plan the new system architecture
3. [Version Determination](05-version-determination.md) - Establish recovery baseline

## Implementation

**New Cluster Initialization**: `lib/bedrock/control_plane/director/recovery/initialization_phase.ex`

## Next Phase

- **New Cluster**: Proceeds to [Vacancy Creation](04-vacancy-creation.md)
- **Existing Cluster**: Proceeds to [Log Recovery Planning](03-log-recovery-planning.md)