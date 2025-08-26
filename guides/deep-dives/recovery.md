# Bedrock's Recovery Architecture: Building Reliability Through Simplicity

When distributed systems fail, engineers face a fundamental choice: patch the immediate problems or rebuild from a trusted foundation. Bedrock chooses reconstruction over repair, rebuilding the entire [transaction processing infrastructure](architecture.md) from verified persistent state when any critical component fails. This approach trades recovery time—typically measured in seconds—for absolute confidence in system integrity.

Bedrock inverts traditional distributed systems priorities, following FoundationDB's architectural principle[^1]: optimize for the worst-case scenario rather than normal operation. When anything goes wrong, assume everything might be compromised and rebuild the entire transaction system. This philosophy rests on three key insights: reliability theory favors simplicity over complex recovery optimization, comprehensive reconstruction enables complete confidence while targeted repairs leave lingering questions about hidden corruption, and complex recovery logic is fundamentally untestable across every possible failure combination. Simple, comprehensive recovery provides a single, well-defined path that can be thoroughly validated against real-world scenarios.

Bedrock's recovery transforms a failed distributed system into trusted operational infrastructure through a carefully orchestrated sequence of phases. Each phase builds upon verified results from the previous stage, creating a logical progression from uncertainty to complete confidence in system integrity.

## Phase 0: [TSL Validation](../quick-reads/recovery/tsl-validation.md)

The TSL validation phase serves as recovery's first line of defense against data corruption by ensuring that recovered Transaction System Layout (TSL) data maintains proper type safety and passes sanity checks after potential corruption or version mismatches. The algorithm systematically validates type conversions across all TSL components, particularly version number representations essential for MVCC operations, and immediately terminates if any validation fails rather than proceeding with potentially corrupted configuration data.

## Phase 1: [Service Locking](../quick-reads/recovery/service-locking.md)

The service locking phase establishes exclusive control over the failed system by solving the fundamental coordination problem of which director has authority to proceed when multiple recovery attempts might occur simultaneously. Recovery locks every service from the previous transaction system layout through epoch-based precedence, instructing them to cease normal operations and report exclusively to the recovery director, while simultaneously determining whether this represents a new cluster initialization or existing system requiring data recovery.

## Phase 2: [Log Recovery Planning](../quick-reads/recovery/log-recovery-planning.md)

The log recovery planning phase determines what committed transaction data can be safely preserved from potentially compromised infrastructure by systematically analyzing Bedrock's natural redundancy architecture. The algorithm examines available logs within each shard, generates all possible combinations meeting quorum requirements, and calculates the recoverable version range using cross-shard minimum calculation to ensure the recovered system maintains strict consistency guarantees while maximizing transaction history preservation.

## Phase 3: [Vacancy Creation](../quick-reads/recovery/vacancy-creation.md)

The vacancy creation phase solves the sophisticated architectural challenge of specifying the complete structure of a new distributed system without prematurely binding that structure to specific machines or processes. Recovery creates abstract vacancy placeholders that separate architectural planning from infrastructure assignment, treating different service types according to their fundamental characteristics: logs receive clean-slate reconstruction regardless of old availability, storage services get ultra-conservative preservation strategies that maintain existing assignments while adding vacancies only when needed, and resolvers receive computational resource planning through descriptors that map key ranges to vacancy placeholders.

## Phase 4: [Version Determination](../quick-reads/recovery/version-determination.md)

The version determination phase establishes the critical mathematical foundation of the highest transaction version guaranteed durable across the entire cluster through rigorous examination of storage team health and replica consistency. The algorithm employs a two-tier approach: calculating fault-tolerant versions within each storage team by selecting versions ensuring quorum availability, then determining the cluster-wide minimum to establish a conservative recovery baseline that accounts for potential replica failures during reconstruction.

## Phase 5: [Log Recruitment](../quick-reads/recovery/log-recruitment.md)

The log recruitment phase embodies Bedrock's core philosophy of choosing reliability over optimization by aggressively creating entirely new log infrastructure rather than attempting to salvage potentially compromised components. The recruitment strategy implements a three-tier approach: discovering existing available log services in the cluster, creating fresh log workers distributed across nodes using round-robin assignment when needed, and establishing exclusive control through comprehensive locking to prevent interference while validating operational readiness.

## Phase 6: [Storage Recruitment](../quick-reads/recovery/storage-recruitment.md)

Storage recruitment represents recovery's most cautious operational phase because storage services contain irreplaceable persistent data that would be expensive or impossible to reconstruct if lost. The algorithm implements a strict preservation hierarchy that never modifies existing services from the old system, only recruiting available services not part of the previous infrastructure to fill specific vacancy gaps, and creating new storage workers with fault-tolerant distribution when candidates prove insufficient, all while ensuring comprehensive validation through exhaustive locking to guarantee zero risk to valuable persistent data.

## Phase 7: [Log Replay](../quick-reads/recovery/log-replay.md)

The log replay phase orchestrates the critical transfer of committed transaction data from the compromised system to newly constructed infrastructure, exemplifying Bedrock's fundamental reliability philosophy of choosing data integrity over operational optimization. The algorithm implements intelligent pairing between new and old log services using round-robin distribution, selectively copying only committed transactions within the established version vector while deliberately excluding uncommitted transactions to ensure the new system begins operation with completely clean, consistent transaction history.

## Phase 8: [Sequencer Startup](../quick-reads/recovery/sequencer-startup.md)

The sequencer startup phase addresses one of distributed systems' most fundamental challenges by establishing a singleton service that provides global transaction ordering authority across the entire cluster. The sequencer initializes on the director's current node using the established recovery baseline as its starting version, ensuring every transaction receives a unique, totally-ordered version number while coordinating extensively with commit proxies to maintain consistent version assignment and provide the timing foundation for accurate component state management.

## Phase 9: [Commit Proxy Startup](../quick-reads/recovery/proxy-startup.md)

The commit proxy startup phase establishes distributed components that provide horizontal scalability for transaction processing workloads while ensuring continued operation even when individual proxies fail. Recovery employs round-robin distribution to deploy configured proxy processes across nodes with coordination capabilities, providing each proxy with comprehensive configuration information including epoch details and director coordinates, while keeping them locked until the system layout phase transitions them to operational mode to prevent premature transaction processing.

## Phase 10: [Resolver Startup](../quick-reads/recovery/resolver-startup.md)

The resolver startup phase handles the sophisticated challenge of preventing conflicting transactions from corrupting data by implementing Multi-Version Concurrency Control (MVCC) conflict detection with mathematical precision. The startup process transforms abstract resolver descriptors into operational processes through sophisticated range-to-storage-team mapping and tag-based log assignment algorithms, while resolvers reconstruct their historical MVCC state by processing transactions from assigned logs to build the conflict detection information required for accurate future conflict evaluation.

## Phase 11: [Transaction System Layout](../quick-reads/recovery/transaction-system-layout.md)

The transaction system layout phase resolves the coordination challenge of isolated components by constructing the authoritative blueprint that enables distributed system components to locate and communicate with each other effectively. Recovery performs final validation of core components and constructs the complete coordination blueprint containing mappings of services to roles and key ranges to teams, preparing for the critical system transaction that will durably store this configuration.

## Phase 12: [Monitoring](../quick-reads/recovery/monitoring.md)

The monitoring phase establishes comprehensive monitoring infrastructure that implements Bedrock's core fail-fast philosophy, covering sequencer, commit proxies, resolvers, logs, and the director with continuous operational validation while deliberately excluding storage servers that handle failures independently. **Critically, monitoring is established before the system transaction to ensure that any component failures during transaction processing trigger immediate fail-fast behavior rather than recovery stalls.**

## Phase 13: [Persistence](../quick-reads/recovery/persistence.md)

The persistence phase durably stores the new system configuration through a system transaction that validates the complete transaction processing pipeline while ensuring the configuration survives future failures and remains available for operational reference. **With monitoring already active, any resolver timeouts or component failures during this critical transaction will trigger proper director shutdown and fresh recovery startup, preventing the system from getting stuck in repair loops.** Once this transaction succeeds, the director transitions from recovery mode to normal operation, officially completing the transformation from system failure state to trusted operational infrastructure.

## From Crisis to Confidence

What began as a system in complete crisis has been systematically transformed into infrastructure that operators can trust completely. Recovery methodically rebuilt everything from verified foundations rather than attempting targeted repairs, creating a system where every component has undergone validation, every piece of data has been accounted for, and every coordination relationship has been established cleanly.

This comprehensive reconstruction delivers absolute confidence in system state integrity that targeted repair approaches struggle to provide. All recoverable data survived the process intact through conservative version determination and careful transaction preservation, ensuring applications can resume operation with no gaps in their transactional history. The cluster now stands ready with clean architectural foundations—storage servers know their responsibilities, logs contain verified histories, and monitoring oversees components constructed to work together rather than patched to coexist.

This represents the fundamental value proposition: trading brief operational downtime for a system built from verified components with transparent operational state, providing the foundation for sustained, trustworthy distributed system operation.

[^1]: FoundationDB's recovery approach is detailed in ["FoundationDB: A Distributed Unbundled Transactional Key Value Store"](https://www.foundationdb.org/files/fdb-paper.pdf) and implemented in Bedrock at `lib/bedrock/control_plane/director/recovery.ex`
