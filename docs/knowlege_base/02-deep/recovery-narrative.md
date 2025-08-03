# Recovery Flow and Rationale

**Understanding Bedrock's comprehensive recovery approach through its complete execution flow.**

Distributed systems face a fundamental choice when components fail: attempt targeted repairs for each failure mode, or rebuild the entire system from a known-good state. Bedrock chooses the latter approach, trading recovery time for system simplicity and reliability.

This document explains the complete recovery flow, the engineering rationale behind each phase, and how this approach delivers better overall reliability than attempting to optimize individual failure scenarios.

## The Philosophy: Worst Case as Best Case

Traditional distributed systems often implement elaborate failure detection, partial recovery mechanisms, and complex repair procedures. Each additional mechanism introduces new failure modes, creating a web of interdependent recovery logic that becomes increasingly difficult to reason about and test.

Bedrock takes inspiration from FoundationDB's approach[^1]: when any critical component fails, rebuild the entire transaction processing layer from a known-good state. This trades complexity for predictability. Recovery might take seconds, but those seconds buy you a system that's easier to understand, test, and trust.

The key insight is that distributed databases spend most of their time in steady state, not recovering. Optimizing for the common case (normal operation) while keeping recovery simple and comprehensive yields better overall reliability than trying to optimize recovery speed at the cost of complexity.

## Recovery Entry Point

Recovery begins when the Director creates a `RecoveryAttempt` with the current timestamp, cluster configuration, and epoch. This initialization occurs in `RecoveryAttempt.new/3` and establishes the timing baseline for the entire recovery process. The recovery attempt tracks all state changes as recovery progresses through its phases.

## Service Discovery and Locking

### Service Locking[^2]

Recovery establishes exclusive control by selectively locking services from the old transaction system layout. Only services that were part of the previous system are candidates for locking—these contain data that must be preserved during recovery. Other available services remain unlocked until recruitment phases assign them to specific roles.

Service locking serves three critical purposes:

1. **Prevent [split-brain](../../glossary.md#split-brain)** - Each service accepts locks from only one director at a time, with epoch-based precedence ensuring exclusive control
2. **Halt transaction processing** - Locked services immediately stop all normal operations (logs stop accepting transactions, storage stops pulling from logs) 
3. **Validate reachability** - The lock operation confirms services are genuinely operational and returns actual process PIDs plus recovery state information (oldest/newest transaction versions, durability status), critical in dynamic environments where machines restart with new addresses

Locking attempts all services in parallel. Individual service failures (unreachable, timeout) are ignored since the system may have failed due to partial component failures. Recovery gathers and locks as many services as possible from the old system, determining in subsequent phases what can be salvaged. However, if any service is already locked with a newer epoch, it rejects the request with `:newer_epoch_exists`, indicating this director has been superseded and must halt recovery entirely. Unlike other failures that cause recovery to stall and retry, epoch conflicts are fatal errors that stop all further recovery attempts. This strict epoch ordering prevents conflicts between concurrent directors.

The recovery path is determined by whether logs existed in the previous generation:
- **No logs in previous layout**: No transaction system existed → initialization required  
- **Logs in previous layout**: Previous system with data → recovery from existing state (regardless of how many services are successfully locked)

This automatic path selection eliminates the need for separate cluster state detection mechanisms. Critically, if logs were defined but none can be locked due to failures, recovery proceeds to data recovery phases rather than initialization - preventing accidental data loss by ensuring existing systems are never mistaken for new deployments.

**Input**: 
- `context.old_transaction_system_layout` - Previous system configuration with logs and storage teams
- `context.available_services` - Currently discoverable services from coordinator
- `recovery_attempt.epoch` - Current recovery epoch for locking precedence  

**Output**: 
- `locked_service_ids` - Set of successfully locked service identifiers
- `log_recovery_info_by_id` - Recovery state information for each locked log service
- `storage_recovery_info_by_id` - Recovery state information for each locked storage service  
- `transaction_services` - Service descriptors with PIDs and status for locked services
- `service_pids` - Direct PID mappings for locked services

## Recovery Path Divergence

Recovery splits into two paths based on locking results, though both converge on service recruitment.

### New Cluster Initialization[^3]

When no logs existed in the previous layout, the system requires initial configuration. The `InitializationPhase` creates the initial transaction system layout for a new cluster, running only when no previous cluster state exists. It creates log vacancy placeholders for the desired number of logs and exactly two storage teams with fixed key range boundaries, using vacancy placeholders instead of assigning specific services immediately.

Log vacancies are assigned initial version ranges `[0, 1]` to establish the starting point for transaction processing. Storage teams are created with a fundamental keyspace division - one team handles user data (empty string to `0xFF`), the other handles system data (`0xFF` to end-of-keyspace). This division separates user keys from system keys, with keys above the user space reserved for system metadata and configuration. Creating vacancies rather than immediate service assignment allows later phases to optimize placement across available nodes and handle assignment failures independently. The phase always succeeds since it only creates in-memory structures.

**Input**: 
- `context.cluster_config.parameters.desired_logs` - Number of log vacancies to create
- `context.cluster_config.parameters.desired_replication_factor` - Number of storage vacancies per team

**Output**: 
- `durable_version` - Set to 0 for new cluster
- `old_log_ids_to_copy` - Empty list (no previous data)
- `version_vector` - Set to {0, 0} as starting point
- `logs` - Map of log vacancies with initial version ranges [0, 1]
- `storage_teams` - Two teams with fixed key range boundaries and vacancy placeholders

### Log Recovery Planning[^4]

When logs existed in the previous layout, the `LogRecoveryPlanningPhase` determines what data can be safely recovered from potentially failed or unavailable logs. During normal operation, Bedrock sends _every_ transaction to _all_ logs in the system to maintain global ordering—every log contains a record of every committed transaction (see [Step 4.6: Durable Log Persistence](transactions-deep.md#step-46-durable-log-persistence)). However, log failures may be the reason recovery is necessary, so not all logs can be guaranteed available or intact.

The algorithm groups available logs by their range tags (shards) and evaluates what data can be safely recovered from each shard. Within each shard, it generates all possible log combinations meeting quorum requirements, calculates recoverable version ranges for each combination, and selects the combination providing the largest data range. The final recovery decision takes the cross-shard minimum of these ranges—only including the highest transaction version guaranteed complete across every participating shard[^18]. By maximizing the transaction history copied to the new generation, we improve the recovered system's tolerance for storage server lag[^19]. If any shard cannot meet its quorum requirements, the phase stalls with `:unable_to_meet_log_quorum`.

**Input**: 
- `context.old_transaction_system_layout.logs` - Previous log layout with log descriptors
- `recovery_attempt.log_recovery_info_by_id` - Recovery information from locked log services
- `context.cluster_config.parameters.desired_logs` - Used to calculate quorum requirements

**Output**: 
- `old_log_ids_to_copy` - List of log IDs selected for data preservation
- `version_vector` - Consistent version bounds {oldest_version, newest_version} for the selected logs

## Service Planning and Recruitment

### Vacancy Creation[^5]

Recovery must plan for a complete new system configuration, but how do you specify what services you need without prematurely committing to specific machines or processes? The `VacancyCreationPhase` solves this planning problem by creating "vacancy" placeholders—numbered slots like `{:vacancy, 1}` that represent service requirements without binding them to actual services. Since Bedrock organizes data into shards (independent partitions) identified by "range tags," services with identical tag sets can substitute for each other, but services with different tag sets serve different data and cannot be interchanged. This shard structure drives the entire vacancy creation process: the phase must ensure each shard combination has enough services to meet fault tolerance requirements while deferring the specific assignment of services to shards until recruitment phases can see all available options.

Recovery takes fundamentally different approaches for logs versus storage because they have different constraints. Logs contain no persistent state—they can be recreated by replaying transactions from other logs—so recovery uses a clean-slate approach, creating the desired number of new log vacancies for each shard combination from the old system. If the old system had logs serving `["shard_a"]` and `["shard_b"]`, and configuration specifies 3 logs per shard, recovery creates 6 log vacancies total, with old logs becoming candidates for filling these vacancies but not automatically preserved. Storage servers, however, contain persistent data that's expensive to migrate, so recovery takes a conservative approach, preserving existing storage assignments and only creating additional vacancies where needed—if shard A has 2 storage servers but configuration requires 3, recovery creates just 1 storage vacancy for shard A.

Resolvers, unlike logs and storage, are purely computational components that maintain transaction conflict detection state. The phase creates resolver descriptors that map each storage team's key range to a resolver vacancy placeholder. These descriptors define the boundaries between resolvers (where one resolver's responsibility ends and another's begins) without committing to specific resolver processes. Each resolver descriptor contains a start key from the storage teams and a vacancy mark, allowing resolver startup to later assign actual resolver processes that will handle MVCC conflict detection for their assigned key ranges.

This vacancy approach separates planning ("what do we need?") from assignment ("which services provide it?"), allowing recruitment phases to see the complete picture—all vacancies needing services and all services available to provide them—enabling optimizations like fault-tolerant placement across machines, minimizing data movement, and load balancing. The result is a complete specification of the desired new system: log vacancies tagged by shard, storage teams with additional vacancy slots, resolver descriptors with vacancy marks, and a clear handoff to recruitment phases that will fill these positions with actual services.

**Input**: 
- `context.old_transaction_system_layout.logs` - Used only to discover what shard combinations (tag sets) existed
- `context.old_transaction_system_layout.storage_teams` - List of storage team descriptors with tags and storage IDs  
- `context.cluster_config.parameters.desired_logs` - Target log count per tag set
- `context.cluster_config.parameters.desired_replication_factor` - Target replicas per storage tag set

**Output**:
- `recovery_attempt.logs` - Map of vacancy IDs to tag lists (original logs are replaced)
- `recovery_attempt.storage_teams` - Storage teams with expanded storage_ids including vacancies
- `recovery_attempt.resolvers` - Resolver descriptors mapping key ranges to resolver vacancies

### Version Determination[^6]

After planning service requirements, recovery faces a critical question: what's the highest transaction version that can be guaranteed durable across the entire cluster? The `VersionDeterminationPhase` answers this by examining storage teams to find the conservative baseline where all data is truly persistent.

The challenge is that storage servers may be at different transaction versions due to processing lag, network delays, or partial failures. Recovery must find the highest version where enough replicas in every storage team have durably committed the data. This becomes the "recovery baseline"—all transactions at or below this version are guaranteed safe, while transactions above it may need to be replayed from logs.

The algorithm works by calculating a fault-tolerant version within each storage team, then taking the minimum across all teams. For each team, it sorts the durable versions of available replicas and selects the quorum-th highest version (where quorum = ⌊replication_factor/2⌋ + 1). This ensures that even if some replicas fail, the quorum can still provide the data. For example, if a team has three replicas at versions [95, 98, 100] and quorum is 2, the durable version is 98—the second-highest version that guarantees two replicas have the data.

Once each team's durable version is calculated, the cluster-wide durable version becomes the minimum across all teams, ensuring that every piece of data is available at that version. Teams are classified as healthy if they have exactly the quorum number of replicas (optimal replication), or degraded if they have fewer replicas (insufficient fault tolerance) or more replicas (resource inefficiency requiring rebalancing). If any team lacks sufficient replicas to meet quorum, recovery stalls since the cluster cannot guarantee data durability.

**Input**: 
- `context.old_transaction_system_layout.storage_teams` - Storage team descriptors with replica assignments
- `recovery_attempt.storage_recovery_info_by_id` - Durable version information from each storage server
- `context.cluster_config.parameters.desired_replication_factor` - Used to calculate quorum requirements

**Output**:
- `recovery_attempt.durable_version` - Highest version guaranteed durable across all teams
- `recovery_attempt.degraded_teams` - List of team tags requiring rebalancing or repair

### Log Recruitment[^7]

With vacancy placeholders defined, recovery now faces the practical challenge: how do you actually assign real services to fill those vacancies? The `LogRecruitmentPhase` transforms the abstract service plan into concrete assignments while navigating the constraints of what services are available and what data must be preserved.

The challenge is balancing efficiency with safety. Recovery wants to reuse existing services to avoid the overhead of creating new ones, but it cannot touch services from the old transaction system since they contain committed transaction data that might be needed if this recovery fails and another one starts. This creates a careful selection process where recovery must find services that exist but weren't part of the previous configuration.

The algorithm follows a three-phase assignment strategy. First, it identifies existing log services that are available but weren't part of the old system—these are ideal candidates since they already exist and have no conflicting data. If there aren't enough existing services to fill all vacancies, recovery creates new log workers, distributing them across available nodes using round-robin assignment to ensure fault tolerance. For example, if recovery needs 4 new workers across 3 nodes, it assigns workers to nodes in sequence: node1, node2, node3, node1. Finally, all recruited services—both existing and newly created—must be locked to establish exclusive control before proceeding.

The locking step is critical because recruited services might be handling other workloads or could be recruited by a competing recovery process. Only when all services are successfully locked can recovery be confident that the log layer is ready for transaction processing. If there aren't enough nodes to create needed workers, the phase stalls until conditions improve. However, if any recruited service is already locked by a director with a newer epoch, recovery immediately halts with `:newer_epoch_exists`—this director has been superseded and must stop all recovery attempts.

**Input**: 
- `recovery_attempt.logs` - Map of vacancy placeholders to shard tags requiring log services
- `context.old_transaction_system_layout.logs` - Services to exclude from recruitment (preserve recovery data)  
- `context.available_services` - Currently discoverable log services from coordinator
- `context.node_capabilities.log` - Nodes capable of running new log workers

**Output**:
- `recovery_attempt.logs` - Vacancies replaced with actual recruited service IDs
- `recovery_attempt.transaction_services` - Service descriptors with lock status and PIDs
- `recovery_attempt.service_pids` - Direct PID mappings for recruited services

### Storage Recruitment[^8]

With storage team vacancies defined, recovery faces a more complex assignment challenge than log recruitment: how do you fill positions that require persistent data without accidentally destroying valuable information? The `StorageRecruitmentPhase` solves this challenge by implementing an ultra-conservative approach that prioritizes data preservation above all else—storage services contain persistent data that's expensive to recreate and potentially irreplaceable if lost.

The fundamental difference from log recruitment lies in the data stakes. Logs contain transaction records that can be replayed from other logs, but storage services contain the actual committed data—user records, system state, and historical information that took time and computation to build. This means recovery must be extremely careful about which storage services it touches and how it handles them. The algorithm therefore follows a strict preservation hierarchy: first preserve all services from the old system (never touch them during recruitment), then reuse any available services that weren't part of the old system, and only create new workers as a last resort.

The assignment process starts by counting all storage vacancies across all teams—these `{:vacancy, n}` placeholders created during vacancy creation. Recovery then identifies candidate services: available storage services that aren't part of the old transaction system and aren't already assigned to non-vacancy positions in the new system. If there are enough candidates to fill all vacancies, recovery assigns them directly. If not, it calculates how many new workers are needed and creates them using round-robin distribution across available nodes for fault tolerance. For example, if recovery needs 5 storage workers across 3 nodes, it assigns them to nodes in sequence: node1, node2, node3, node1, node2.

Finally, all recruited services—both existing and newly created—must be successfully locked before recovery can proceed. This establishes exclusive control and prevents interference from other processes or competing directors. The phase stalls if there aren't enough nodes to create needed workers or if any recruited service fails to lock. However, if any recruited service is already locked by a director with a newer epoch, recovery immediately halts with `:newer_epoch_exists`—this director has been superseded and must stop all recovery attempts.

**Input**: 
- `recovery_attempt.storage_teams` - Storage team descriptors with vacancy placeholders requiring services
- `context.old_transaction_system_layout.storage_teams` - Services to exclude from recruitment (preserve data)
- `context.available_services` - Currently discoverable storage services from coordinator  
- `context.node_capabilities.storage` - Nodes capable of running new storage workers

**Output**:
- `recovery_attempt.storage_teams` - Vacancies replaced with actual recruited service IDs
- `recovery_attempt.transaction_services` - Service descriptors with lock status and PIDs
- `recovery_attempt.service_pids` - Direct PID mappings for recruited services

## Data Migration

### Log Replay[^9]

With new log services recruited and ready, recovery faces a critical data migration challenge: how do you transfer the current window of committed transactions from the old transaction system to the new one without losing data or breaking consistency? The `LogReplayPhase` solves this by copying transactions from the logs selected during planning to the newly recruited logs, ensuring that storage servers can continue processing from where they left off.

Bedrock completely replaces logs with each recovery generation for robustness—the old logs could have bad disks, corruption, or other storage issues that only become apparent when reading them. Since recovery must read the transaction data anyway to verify what's recoverable, it writes that data to known-good storage in the new logs rather than trusting the old storage. The new transaction system layout will be completely different—different log services, node assignments, potentially different durability policies, and often a different number of logs total. This duplication ensures that all essential transaction data is stored on verified, reliable storage before the new system begins operation.

The replay algorithm pairs each new log with source logs from the old system using round-robin assignment. If there are fewer old logs than new ones (common when scaling up), the algorithm cycles through the old logs repeatedly—for example, with 2 old logs and 4 new logs, the pairing would be: new1←old1, new2←old2, new3←old1, new4←old2. If no old logs are available (during initialization), new logs are paired with `:none` and remain empty until normal transaction processing begins. Each pairing operates efficiently in parallel, copying the established version range from its assigned old log to its target new log.

Only committed transactions within the version vector are copied—uncommitted transactions are discarded since they were never guaranteed durable. If any log copying fails, the phase stalls with detailed failure information. However, if any log reports `:newer_epoch_exists`, the phase immediately returns an error since this director has been superseded and must halt all recovery attempts.

**Input**: 
- `recovery_attempt.old_log_ids_to_copy` - Log IDs selected during log recovery planning phase
- `recovery_attempt.logs` - New log service assignments from recruitment 
- `recovery_attempt.version_vector` - Version range `{first_version, last_version}` for committed data
- `recovery_attempt.service_pids` - Process IDs for all locked log services

**Output**: 
- Successful transition to data distribution phase with new logs populated with transaction data
- Or stall with `{:failed_to_copy_some_logs, failure_details}` for service issues
- Or immediate halt with `:newer_epoch_exists` for epoch conflicts

## Component Startup

### Sequencer Startup[^10]

How does a distributed system ensure that every transaction gets a unique, totally-ordered version number that all components agree on? The `SequencerStartupPhase` solves this fundamental ordering problem by starting the sequencer component—a singleton service that provides the authoritative source of transaction version numbers. Without this global ordering, transaction processing would collapse into chaos as different components assign conflicting version numbers to concurrent transactions.

The sequencer is a critical singleton component where only one instance runs cluster-wide to ensure consistent version assignment. The phase starts the sequencer process on the director's current node with the last committed version from the version vector as the starting point. The sequencer will assign version numbers incrementally from this baseline for new transactions, ensuring no gaps or overlaps in the version sequence.

The phase immediately halts recovery with a fatal error if the sequencer fails to start, since version assignment is fundamental to transaction processing. Unlike temporary resource shortages that can be retried, sequencer startup failure on the director's own node indicates serious system problems requiring immediate attention. Global version assignment requires this singleton sequencer initialized with the current version baseline to ensure transaction ordering consistency across the entire cluster.

**Input**: 
- `recovery_attempt.epoch` - Current recovery epoch for coordination
- `recovery_attempt.version_vector` - Version range with last committed version as baseline
- `recovery_attempt.cluster.otp_name(:sequencer)` - OTP name for service registration

**Output**: 
- `recovery_attempt.sequencer` - PID of operational sequencer ready to assign transaction versions

See [Sequencer documentation](../../components/sequencer.md) for detailed component behavior and version assignment algorithms.

### Proxy Startup[^11]

The `ProxyStartupPhase` starts commit proxy components that provide horizontal scalability for transaction processing. Multiple proxies can handle increasing transaction volume without creating bottlenecks at individual components.

The phase uses round-robin distribution to start the desired number of commit proxy processes across nodes with coordination capabilities from `context.node_capabilities.coordination`. This ensures fault tolerance by spreading proxies across different machines rather than concentrating them on adjacent nodes. Each proxy is configured with the current epoch and director information, enabling them to detect epoch changes and coordinate with the recovery system. The phase stalls if no coordination-capable nodes are available, since at least one commit proxy must be operational for the cluster to accept transactions.

Importantly, proxies remain locked during startup until the Transaction System Layout phase transitions them to operational mode using their lock tokens. This prevents premature transaction processing before the complete system is ready.

**Input**: 
- `context.cluster_config.parameters.desired_commit_proxies` - Target number of commit proxy processes
- `context.node_capabilities.coordination` - Nodes capable of running coordination components including commit proxies
- `recovery_attempt.epoch` - Current recovery epoch for coordination
- `context.lock_token` - Lock token for proxy startup coordination

**Output**: 
- `recovery_attempt.proxies` - List of started commit proxy PIDs ready for transaction coordination

See [Commit Proxy documentation](../../components/commit-proxy.md) for detailed component behavior, batching strategies, and transaction coordination mechanisms.

### Resolver Startup[^12]

How does a distributed database prevent conflicting transactions from corrupting data when multiple clients modify overlapping key ranges simultaneously? The `ResolverStartupPhase` solves this critical concurrency control challenge by starting resolver components that implement MVCC (Multi-Version Concurrency Control) conflict detection through a sophisticated multi-step process.

The phase transforms the abstract resolver descriptors generated during vacancy creation into operational resolver processes that understand exactly which storage teams they coordinate with and which transaction logs contain relevant historical data. This requires complex range-to-storage-team mapping and tag-based log assignment to ensure each resolver gets precisely the data it needs for conflict detection.

**Range Generation and Storage Team Mapping**: The phase first converts resolver start keys into concrete key ranges by sorting descriptors and creating adjacent ranges covering the entire keyspace. Each resolver range is then mapped to storage team tags using sophisticated range overlap detection—this determines which storage teams each resolver must coordinate with for conflict detection.

**Tag-Based Log Assignment**: Resolvers are assigned logs whose range tags intersect with their storage team tags through a complex filtering algorithm. This ensures each resolver receives transaction data relevant to its key range while avoiding unnecessary data transfer. The algorithm uses tag-based filtering with deduplication to create minimal but complete log sets for each resolver.

**Node Assignment and Recovery**: Resolvers are distributed across resolution-capable nodes using round-robin assignment for fault tolerance. Each resolver starts with a lock token and recovers its historical conflict detection state by processing transactions from assigned logs within the established version vector range—this builds the MVCC tracking information needed to detect future conflicts.

The phase stalls if no resolution-capable nodes are available or if individual resolver startup fails, since conflict detection is fundamental to transaction isolation guarantees.

**Input**: 
- `recovery_attempt.resolvers` - Resolver descriptors from vacancy creation with start keys
- `recovery_attempt.storage_teams` - Storage team descriptors with key ranges and tags  
- `recovery_attempt.logs` - Log descriptors with tag assignments
- `recovery_attempt.service_pids` - Running log processes for historical data recovery
- `context.node_capabilities.resolution` - Nodes capable of running resolver components
- `recovery_attempt.version_vector` - Version range for historical conflict state recovery
- `context.lock_token` - Lock token for resolver coordination

**Output**: 
- `recovery_attempt.resolvers` - List of `{start_key, resolver_pid}` tuples for operational resolvers ready for conflict detection

## System Construction

### Transaction System Layout[^13]

The `TransactionSystemLayoutPhase` creates the Transaction System Layout (TSL)—the blueprint that enables distributed system components to find and communicate with each other. This phase solves the fundamental coordination problem: without a TSL, operational components exist in isolation, unable to coordinate transaction processing.

Recovery has started all the individual components—sequencer, commit proxies, resolvers, logs, and storage servers—but they cannot yet work together. The sequencer doesn't know which logs to notify about new versions. Commit proxies don't know which resolvers handle conflict detection for specific key ranges. Storage servers don't know which logs contain the transactions they need.

The TSL construction process validates that core transaction components remain operational, builds the complete coordination blueprint, and prepares services for the system transaction that will persist this configuration. The validation is lightweight since previous phases already established communication with all components—it primarily confirms nothing failed between startup and TSL construction.

Recovery stalls if validation detects missing components, TSL construction fails, or service unlocking encounters problems, indicating topology issues requiring manual intervention.

See [Transaction System Layout](../../transaction-system-layout.md) for complete TSL specification including data structure, service mapping, and coordination details.

### Persistence[^14]

The `PersistencePhase` persists cluster configuration through a complete system transaction. It constructs a system transaction containing the full cluster configuration and submits it through the entire data plane pipeline. This simultaneously persists the new configuration and validates that all transaction components work correctly.

The phase stores configuration in both monolithic and decomposed formats. Monolithic keys support coordinator handoff while decomposed keys allow targeted component access. The system transaction uses a dual storage strategy: monolithic keys store complete configurations for coordinator handoff scenarios, while decomposed keys store individual components for targeted access.

If the system transaction fails, the director exits immediately rather than retrying. System transaction failure indicates fundamental problems that require coordinator restart with a new epoch. This redundancy ensures that configuration information is available regardless of how future processes need to access it.

**Input**: Complete system configuration and operational transaction processing components  
**Output**: Durably persisted configuration validated through successful system transaction  
**Rationale**: System transaction validates transaction pipeline while durably storing configuration; failure indicates fundamental problems requiring restart

## Operational Monitoring

### Monitoring[^15]

The `MonitoringPhase` sets up monitoring of all transaction system components and marks recovery as complete. It establishes process monitoring for sequencer, commit proxies, resolvers, logs, and storage servers. Any failure of these critical components will trigger immediate director shutdown and recovery restart.

This monitoring implements Bedrock's fail-fast philosophy—rather than attempting complex error recovery, component failures cause the director to exit and let the coordinator restart recovery with a new epoch. The monitoring setup represents the final step before the cluster becomes operational.

Storage servers are notably excluded from monitoring. They handle failures independently through data distribution and repair mechanisms. The transaction processing components, however, must maintain strict consistency, so any failure requires complete recovery. Once monitoring is active, the director shifts from recovery mode to operational mode.

**Input**: Complete transaction system layout with all component PIDs  
**Output**: Process monitoring established for all critical components; recovery marked as complete  
**Rationale**: Fail-fast monitoring for transaction components maintains consistency while excluding storage components that handle failures independently

## The Completed Journey: From Chaos to Order

At the end of this journey, Bedrock has transformed from a potentially inconsistent state with failed components into a fully operational, consistent, and monitored distributed database. The recovery process touched every aspect of the system: data preservation, service assignment, component startup, configuration persistence, and operational monitoring.

The key to this transformation is that recovery solves the worst case scenario comprehensively. Rather than trying to patch around individual failures with specialized repair logic, recovery rebuilds the entire transaction processing layer from first principles. This approach trades recovery time for system simplicity and reliability.

When recovery completes, operators have a system they can trust: every component is operational, all data has been preserved and is accessible, configuration is durably persisted, and monitoring is in place to detect future problems. The system is not just functional—it's in a known-good state that serves as a solid foundation for continued operation.

This is the essence of "solving the worst case, always": by comprehensively rebuilding rather than incrementally repairing, Bedrock achieves reliability through simplicity rather than complexity through optimization.

[^1]: FoundationDB's approach described in ["FoundationDB: A Distributed Unbundled Transactional Key Value Store"](https://www.foundationdb.org/files/fdb-paper.pdf) and implemented in `lib/bedrock/control_plane/director/recovery.ex`

[^2]: `lib/bedrock/control_plane/director/recovery/locking_phase.ex` - Service locking and epoch-based generation management

[^3]: `lib/bedrock/control_plane/director/recovery/initialization_phase.ex` - New cluster transaction system layout creation

[^4]: `lib/bedrock/control_plane/director/recovery/log_recovery_planning_phase.ex` - Log recovery planning and version vector determination

[^5]: `lib/bedrock/control_plane/director/recovery/vacancy_creation_phase.ex` - Service requirement analysis and placeholder creation

[^6]: `lib/bedrock/control_plane/director/recovery/version_determination_phase.ex` - Durable version calculation and storage team health assessment

[^7]: `lib/bedrock/control_plane/director/recovery/log_recruitment_phase.ex` - Log service assignment and worker creation

[^8]: `lib/bedrock/control_plane/director/recovery/storage_recruitment_phase.ex` - Storage service assignment with data preservation

[^9]: `lib/bedrock/control_plane/director/recovery/log_replay_phase.ex` - Transaction data migration between log configurations

[^10]: `lib/bedrock/control_plane/director/recovery/sequencer_startup_phase.ex` - Global sequencer initialization with version baseline

[^11]: `lib/bedrock/control_plane/director/recovery/proxy_startup_phase.ex` - Distributed commit proxy startup and configuration

[^12]: `lib/bedrock/control_plane/director/recovery/resolver_startup_phase.ex` - MVCC resolver initialization with historical data recovery

[^13]: `lib/bedrock/control_plane/director/recovery/transaction_system_layout_phase.ex` - Complete system blueprint construction with integrated validation and service unlocking

[^14]: `lib/bedrock/control_plane/director/recovery/persistence_phase.ex` - Configuration persistence through system transaction validation

[^15]: `lib/bedrock/control_plane/director/recovery/monitoring_phase.ex` - Fail-fast process monitoring establishment

[^18]: This shard-aware recovery approach differs architecturally from FoundationDB's method described in ["FoundationDB: A Distributed Unbundled Transactional Key Value Store"](https://www.foundationdb.org/files/fdb-paper.pdf). FoundationDB uses Known Committed Version (KCV) tracking by commit proxies, where recovery calculates the minimum of Durable Versions (DV) from logs but constrains the recoverable range by what proxies have confirmed as committed through KCV advancement. This creates a "version lag" issue: if a transaction successfully persists to all logs but no subsequent transaction occurs to advance the KCV, that transaction is discarded during recovery despite being fully durable. Bedrock directly examines log persistence across shards without proxy-dependency, recovering any transaction that achieved cross-shard quorum persistence regardless of subsequent proxy activity, thus avoiding the version lag limitation while ensuring stronger consistency guarantees.

[^19]: Storage servers pull transaction data from logs to maintain their local state. When storage servers are behind the recovered log baseline, they must process more transactions to catch up. By copying from all available logs instead of a minimal subset, recovery preserves more historical transaction data in the new log generation. This extended history provides storage servers with more opportunities to find their current position and reduces the risk that lagging storage servers will fall behind the available log data during their own recovery process.