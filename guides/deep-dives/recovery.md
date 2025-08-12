# Bedrock's Recovery System: Building Reliability Through Simplicity

**How a distributed key-value store achieves bulletproof recovery by rebuilding rather than repairing.**

When distributed systems fail, engineers face a fundamental choice: patch the immediate problems or rebuild from a trusted foundation. Most systems choose patching—implementing elaborate detection mechanisms, targeted repairs, and complex state reconciliation logic. Each patch introduces new failure modes, creating increasingly brittle recovery procedures that become impossible to test comprehensively.

Bedrock chooses a different path. When any critical component fails, the system rebuilds the entire [transaction processing infrastructure](architecture.md) from verified persistent state. This comprehensive reconstruction trades recovery time—typically measured in seconds—for absolute confidence in system integrity.

## The Philosophy: Optimizing for Confidence

Traditional distributed systems optimize for the common case: normal operation when everything functions correctly. They implement sophisticated failure detection, partial recovery mechanisms, and targeted repair procedures for specific failure scenarios. While logical, this approach creates an expanding web of interdependent recovery logic that becomes extraordinarily difficult to reason about, test thoroughly, or trust completely.

Bedrock inverts this priority, following FoundationDB's architectural principle[^1]: optimize for the worst-case scenario. When anything goes wrong, assume everything might be compromised and rebuild the entire transaction system. This philosophy rests on several key insights:

**Reliability theory favors simplicity**: A distributed database should spend the vast majority of its operational lifetime in steady state, not recovering from failures. Optimizing for normal operation while maintaining simple recovery procedures should yield superior overall reliability compared to complex recovery optimization.

**Comprehensive recovery enables complete confidence**: After a failure, operators need absolute certainty that the system is genuinely healthy. Targeted repairs leave lingering questions about hidden corruption, incomplete fixes, or subtle inconsistencies. Comprehensive reconstruction eliminates these concerns by building a verified system from trusted components.

**Complex recovery logic is fundamentally untestable**: You cannot practically simulate every possible combination of partial failures, network partitions, timing edge cases, and concurrent recovery attempts. Simple, comprehensive recovery provides a single, well-defined path through the system that can be thoroughly validated against real-world failure scenarios.

## The Recovery Architecture: Systematic Reconstruction

Bedrock's recovery transforms a failed distributed system into a trusted, operational infrastructure through a carefully orchestrated sequence of phases. Each phase builds upon verified results from the previous stage, creating a logical progression from uncertainty to complete confidence in system integrity.

### Phase 0: TSL Type Safety Validation

Before recovery can safely proceed with any service coordination, it must address a fundamental data integrity challenge: ensuring that recovered Transaction System Layout (TSL) data maintains proper type safety after potential corruption or version mismatches. The [TSL validation phase](../quick-reads/recovery/tsl-validation.md) serves as recovery's first line of defense against data corruption that could compromise the entire reconstruction process.

The validation phase examines critical type conversions within the recovered TSL data structure, particularly focusing on version number representations that are essential for MVCC operations. Version numbers may be stored as integers in some contexts but must be converted to binary format for certain lookup operations. Type mismatches in these conversions can cause silent failures in MVCC conflict detection, leading to consistency violations that would be extremely difficult to detect and debug.

The algorithm systematically validates type conversions across all TSL components, ensuring that version ranges, storage team configurations, and service assignments maintain proper type safety. If any validation fails, recovery immediately terminates rather than proceeding with potentially corrupted configuration data that could introduce subtle but catastrophic consistency violations.

Upon successful validation, the phase transitions to service locking for existing clusters or directly to initialization for new deployments, establishing the foundation for reliable recovery execution.

### Phase 1: Establishing Authority Through Service Locking

Recovery begins by solving a fundamental coordination problem: when multiple [directors](../glossary.md#director) might attempt recovery simultaneously after a system-wide failure, which one has authority to proceed? The [service locking phase](../quick-reads/recovery/service-locking.md) establishes this exclusive control through epoch-based precedence and targeted component isolation, while simultaneously determining whether this is a new cluster initialization or an existing system requiring data recovery.

The locking protocol operates like a controlled demolition site. Before rebuilding begins, you must secure the area, halt all conflicting activities, and establish clear authority over the resources involved. Recovery locks every service that was part of the previous [transaction system layout](../quick-reads/transaction-system-layout.md), instructing them to immediately cease normal operations and report exclusively to the recovery director.

This initial phase accomplishes three essential objectives that enable all subsequent recovery work:

**Preventing split-brain scenarios**: Each service accepts locks from only one director at a time, with higher epoch numbers taking precedence. This ensures that competing recovery attempts cannot create conflicting system states.

**Halting transaction processing**: Locked services immediately stop accepting new transactions, preventing further data corruption or inconsistency while recovery proceeds. This creates a stable foundation for reconstruction.

**Validating component reachability**: The locking operation confirms that services are genuinely operational, returning critical state information including current transaction versions, durability status, and process identifiers needed for subsequent phases.

The locking strategy reveals a crucial design principle: only services containing committed transaction data from the previous system become candidates for locking. These components hold valuable persistent state that must be preserved during recovery. Other available services remain unlocked, ready for recruitment into the new system architecture.

Recovery attempts to lock all previous-generation services in parallel, gracefully handling individual component failures. Since partial component failures may have triggered recovery, the system gathers and secures as many services as possible, deferring decisions about what can be salvaged to later phases. However, if any service reports being locked by a director with a newer epoch, recovery immediately terminates—indicating that a more recent recovery attempt has already superseded this one.

### Phase 2: Transaction Data Recovery Analysis

For existing clusters, recovery must solve one of distributed systems' most sophisticated challenges: determining what committed transaction data can be safely preserved from potentially compromised infrastructure. The [log recovery planning algorithm](../quick-reads/recovery/log-recovery-planning.md) addresses this through systematic analysis of Bedrock's natural redundancy architecture.

Bedrock's transaction design creates inherent data safety through a fundamental principle: every committed [transaction](../quick-reads/transactions.md) is durably stored on every log in the system. This design means each [log](../components/data-plane/log.md) contains a complete, authoritative record of the transaction history. However, since log failures may have triggered recovery, not all logs can be assumed available or intact.

The recovery planning algorithm approaches this challenge with mathematical rigor. Logs are organized into shards that enable parallel processing, and recovery must ensure each shard can provide sufficient data to guarantee consistency. The algorithm systematically examines available logs within each shard, generates all possible combinations meeting quorum requirements, and calculates the recoverable version range for each viable combination.

Consider a practical scenario: a system with six logs distributed across two shards, where each shard requires two logs for quorum. Shard Alpha contains logs 1, 2, and 3, while Shard Beta contains logs 4, 5, and 6. If logs 1, 2, 4, and 5 remain available with complete transaction data through version 100, while logs 3 and 6 are unreachable, recovery can safely reconstruct the system through version 100—both shards achieve their quorum requirements with the available logs.

The final recovery decision employs cross-shard minimum calculation: recovery preserves only the highest transaction version guaranteed complete across every participating shard. This conservative approach ensures the recovered system maintains strict consistency guarantees while maximizing the amount of transaction history available to the new generation.

### Phase 3: Architectural Planning Through Vacancy Abstraction

Recovery must now solve a sophisticated architectural challenge: specifying the complete structure of a new distributed system without prematurely binding that structure to specific machines or processes. The solution lies in one of Bedrock's most elegant design patterns: [vacancy placeholders](../quick-reads/recovery/vacancy-creation.md) that separate architectural planning from infrastructure assignment.

Rather than immediately mapping concrete services to specific roles, recovery creates abstract vacancy slots like `{:vacancy, 1}`, `{:vacancy, 2}`, and `{:vacancy, 3}` that represent service requirements without committing to particular infrastructure. This abstraction proves invaluable in practice—the planning phase addresses "what architecture do we need?" while subsequent recruitment phases handle "which available services can fulfill these requirements?"

The vacancy creation algorithm treats different service types according to their fundamental operational characteristics and data preservation requirements:

**Log Services: Clean Slate Reconstruction**  
Logs contain no persistent state beyond transaction records that exist identically in other logs, enabling aggressive reconstruction strategies. Recovery employs a clean-slate approach regardless of old log availability. If the previous system had logs serving specific shard combinations and the current configuration specifies three logs per shard, recovery creates exactly three log vacancies for each shard combination. The algorithm prioritizes architectural consistency over preservation of potentially compromised log infrastructure.

**Storage Services: Ultra-Conservative Preservation**  
[Storage](../components/data-plane/storage.md) services present fundamentally different challenges because they contain persistent data that would be expensive and time-consuming to recreate. Recovery implements an ultra-conservative preservation strategy, maintaining all existing storage assignments while creating additional vacancy slots only when configuration requirements exceed current replication levels. If a storage team currently operates with two replicas but configuration demands three, recovery creates exactly one storage vacancy for that team—never disturbing existing, valuable data.

**Resolver Services: Computational Resource Planning**  
[Resolvers](../components/data-plane/resolver.md) require specialized treatment as purely computational components that maintain [MVCC](../glossary.md#multi-version-concurrency-control) conflict detection state derived from transaction logs. Recovery creates resolver descriptors that map each storage team's key range to corresponding resolver vacancies. These descriptors define computational boundaries and responsibility areas without committing to specific resolver processes, enabling the subsequent startup phase to optimize resolver placement and resource allocation.

### Phase 4: Establishing the Durability Baseline

Before infrastructure reconstruction can begin, recovery must establish a critical mathematical foundation: the highest transaction [version](../glossary.md#version) guaranteed durable across the entire cluster. The [version determination process](../quick-reads/recovery/version-determination.md) represents one of recovery's most sophisticated analytical challenges, requiring rigorous examination of storage team health and replica consistency.

The fundamental challenge arises from the reality of distributed operation: [storage](../components/data-plane/storage.md) servers inevitably operate at slightly different transaction versions due to processing delays, network latency, or partial component failures. Some replicas remain current with the latest committed transactions, while others lag behind by varying amounts. Recovery must identify a conservative durability baseline where sufficient replicas have committed the data, guaranteeing that the reconstructed system can provide consistent access to all transactions at or below this established version.

The algorithm employs a two-tier mathematical approach: calculate fault-tolerant versions within each storage team, then determine the cluster-wide minimum to establish the recovery baseline.

**Storage Team Analysis**  
For each storage team, recovery examines durable versions reported by available replicas, sorts them in ascending order, and selects the version ensuring quorum availability. Consider a three-replica team with replicas at versions [95, 98, 100] where quorum requires two replicas. The team's durable version becomes 98—the second-highest version guaranteeing that two replicas possess the data even if the most current replica fails during reconstruction.

**Cluster-Wide Baseline Calculation**  
Once individual team durability is established, the cluster-wide durable version emerges as the minimum across all teams. This conservative approach ensures every piece of data throughout the cluster remains available at the recovery baseline, accounting for potential replica failures during the new system's startup sequence.

**Team Health Classification**  
The process simultaneously categorizes storage teams as healthy, degraded, or insufficient based on replica availability relative to desired replication factors. This classification provides crucial information for subsequent load rebalancing and resource allocation phases.

### Phase 5: Infrastructure Recruitment and Deployment

With architectural planning complete, recovery transitions from abstract design to concrete infrastructure construction. The recruitment phases transform vacancy placeholders into operational services, employing fundamentally different strategies for each service type based on their data preservation requirements and operational characteristics.

#### Log Service Recruitment: Prioritizing Reliability Over Preservation

[Log recruitment](../quick-reads/recovery/log-recruitment.md) embodies Bedrock's core philosophy of choosing reliability over optimization. Since logs contain no persistent state beyond transaction records that exist identically in other logs, recovery can aggressively create entirely new log infrastructure rather than attempting to salvage potentially compromised components from the failed system.

The recruitment strategy implements a three-tier approach designed to balance deployment efficiency with fault tolerance:

**Existing Service Discovery**: Recovery first identifies available log services that exist in the cluster but weren't part of the previous transaction system. These services represent optimal candidates—they operate as running processes without carrying potentially corrupted data from the failed infrastructure.

**New Service Creation**: When existing services cannot fill all vacancy positions, recovery creates fresh log workers and distributes them across available nodes using round-robin assignment. This distribution strategy ensures fault tolerance by spreading critical components across different machines rather than concentrating them where they might share common failure modes.

**Exclusive Control Establishment**: The recruitment process concludes with comprehensive locking of all recruited services, establishing exclusive control to prevent interference from competing recovery attempts while validating that recruited services remain genuinely operational and responsive.

#### Storage Service Recruitment: Ultra-Conservative Data Preservation

[Storage recruitment](../quick-reads/recovery/storage-recruitment.md) represents recovery's most cautious operational phase because storage services contain irreplaceable persistent data—user records, system state, and historical information that would be expensive or impossible to reconstruct if lost.

The recruitment algorithm implements a strict preservation hierarchy that prioritizes data safety above all other operational concerns:

**Absolute Preservation of Existing Data**: Recovery never modifies or reassigns services from the old transaction system during recruitment, ensuring their valuable persistent data remains completely undisturbed and available for potential future recovery scenarios.

**Targeted Vacancy Filling**: Only services available in the cluster but not part of the previous system become candidates for recruitment. The assignment process systematically counts storage vacancies across all teams—representing gaps between current replica availability and desired replication factors—then identifies suitable candidates to fill these specific gaps.

**Conservative Resource Allocation**: When available candidates prove insufficient, recovery calculates precise requirements for new storage workers and creates them using the same fault-tolerant distribution strategy employed for log services. This measured approach prevents both resource waste and inadequate coverage.

**Comprehensive Validation**: Like log recruitment, the process concludes with exhaustive locking of all recruited services to establish exclusive control and validate operational readiness. This ultra-conservative methodology ensures that valuable persistent data faces zero risk of accidental destruction during the recovery process.

### Phase 6: Transaction Data Migration Through Log Replay

With infrastructure deployment complete, recovery confronts the critical challenge of transferring committed transaction data from the compromised system to the newly constructed infrastructure. The [log replay phase](../quick-reads/recovery/log-replay.md) orchestrates this data migration while simultaneously validating the operational integrity of the new log architecture.

Bedrock's approach to data migration exemplifies its fundamental reliability philosophy: choose data integrity over operational optimization. Despite old logs containing correct transaction data, recovery systematically copies this information to newly recruited logs rather than attempting to reuse potentially compromised storage. This deliberate duplication ensures all essential transaction data resides on verified, reliable infrastructure before the new system assumes operational responsibility.

The replay algorithm implements intelligent pairing between new and old log services using balanced distribution strategies:

**Round-Robin Source Assignment**: Each new log receives assignment to source logs from the old system using round-robin distribution. When the new system contains more logs than the old one—a common scenario during scaling operations—the algorithm cycles through old logs repeatedly to ensure balanced load distribution.

**Practical Distribution Example**: A system transitioning from two old logs to four new logs creates the following pairings: new log 1 copies from old log 1, new log 2 copies from old log 2, new log 3 copies from old log 1, and new log 4 copies from old log 2. This approach ensures even data distribution while accommodating architectural scaling.

**Selective Transaction Copying**: Only committed transactions within the established version vector undergo copying during replay. Uncommitted transactions face deliberate exclusion since they were never guaranteed durable storage and their inclusion could compromise system consistency. This selective approach ensures the new system begins operation with a completely clean, consistent transaction history.

### Phase 7: Transaction Processing Component Orchestration

With data migration complete, recovery initiates the sophisticated process of starting distributed transaction processing components. Each component fulfills a specific role in the transaction pipeline, requiring carefully orchestrated startup sequences to ensure proper coordination while avoiding race conditions that could compromise system integrity.

#### Sequencer Deployment: Establishing Global Transaction Ordering

The [sequencer startup phase](../quick-reads/recovery/sequencer-startup.md) addresses one of distributed systems' most fundamental challenges: ensuring every transaction receives a unique, totally-ordered [version](../glossary.md#version) number that all components recognize as authoritative. Bedrock's [sequencer](../components/data-plane/sequencer.md) component—a singleton service—provides this critical global ordering authority across the entire cluster.

Reliable global ordering demands a single authoritative source to prevent version conflicts that would compromise transaction consistency. The sequencer initializes on the director's current node, using the established recovery baseline as its starting version. From this foundation, the sequencer will assign version numbers incrementally for new transactions, guaranteeing no gaps or overlaps in the global version sequence.

The sequencer's responsibilities extend far beyond simple number assignment. It coordinates extensively with [commit proxies](../components/data-plane/commit-proxy.md) to ensure consistent version assignment across the distributed system while providing the timing foundation that enables other components to maintain accurate state information.

#### Commit Proxy Deployment: Enabling Horizontal Transaction Scalability

The [proxy startup phase](../quick-reads/recovery/proxy-startup.md) establishes [commit proxy](../components/data-plane/commit-proxy.md) components that provide horizontal scalability for transaction processing workloads. Multiple commit proxies can handle increasing transaction volumes without creating bottlenecks at individual components, while their distributed architecture ensures continued transaction processing even when individual commit proxies experience failures.

Recovery employs round-robin distribution to deploy the configured number of commit proxy processes across nodes with coordination capabilities. This geographical distribution strategy ensures fault tolerance by spreading critical transaction processing components across different machines rather than concentrating them in potentially vulnerable single locations.

Each commit proxy receives comprehensive configuration information including current epoch details and director coordinates, enabling them to detect epoch changes and coordinate appropriately with the recovery system. Commit proxies remain locked during startup until the system layout phase transitions them to operational mode, preventing premature transaction processing before complete system readiness.

#### Resolver Deployment: Implementing MVCC Conflict Detection

The [resolver startup phase](../quick-reads/recovery/resolver-startup.md) handles one of distributed transaction processing's most sophisticated challenges: preventing conflicting transactions from corrupting data when multiple clients simultaneously modify overlapping key ranges.

Bedrock's resolver components implement [Multi-Version Concurrency Control (MVCC)](../glossary.md#multi-version-concurrency-control) conflict detection with mathematical precision. Each resolver manages specific key ranges while maintaining detailed conflict detection state that tracks which transactions have accessed which keys at which versions.

The startup process transforms abstract resolver descriptors created during vacancy planning into operational resolver processes. This transformation requires sophisticated range-to-storage-team mapping and tag-based log assignment algorithms to ensure each resolver receives precisely the data it requires for accurate conflict detection.

**MVCC State Recovery**: Resolvers must reconstruct their historical MVCC state by processing transactions from assigned logs within the established version vector range. This recovery process builds the conflict detection information required to evaluate future transaction conflicts accurately, ensuring MVCC guarantees remain intact across the recovery boundary.

### Phase 8: System Layout Construction and Validation

All transaction processing components now operate as individual services, but they exist in complete isolation—the sequencer cannot identify which logs to notify about new versions, commit proxies lack knowledge of which resolvers handle conflict detection for specific key ranges, and storage servers remain unaware of which logs contain their required transaction data. Recovery resolves this coordination challenge through construction and persistence of the authoritative [Transaction System Layout](../quick-reads/recovery/transaction-system-layout.md).

The [Transaction System Layout](../quick-reads/transaction-system-layout.md) functions as the comprehensive coordination blueprint enabling distributed system components to locate and communicate with each other effectively. This critical data structure contains complete mappings of services to operational roles, assignments of key ranges to storage teams, associations between resolvers and their responsibility areas, and all coordination information essential for distributed transaction processing.

**System Validation and Blueprint Construction**  
Recovery performs final validation that all core transaction components remain operational, then constructs the complete coordination blueprint. This validation remains lightweight since previous phases established communication with all components—it primarily confirms that no failures occurred between startup completion and layout construction.

**Transactional Persistence and Pipeline Validation**  
The layout construction process culminates in a comprehensive system transaction that stores the new configuration directly within the distributed database itself. This persistence step serves dual critical purposes: it durably stores the new system configuration for future operational reference, while simultaneously validating that the entire transaction processing pipeline functions correctly end-to-end.

The system transaction exercises the complete transactional data path: client request initiation, commit proxy coordination handling, resolver conflict detection analysis, sequencer version assignment processing, log persistence operations, and storage commitment finalization. This comprehensive pipeline validation ensures every critical component can coordinate effectively within the new system architecture.

**Fail-Fast Error Handling**  
If this system transaction experiences any failure, recovery immediately recognizes that something is fundamentally compromised within the new system configuration. Rather than attempting complex diagnostic repairs, recovery terminates immediately, enabling the coordinator to restart recovery with a fresh [epoch](../glossary.md#epoch). This fail-fast approach prevents deployment of potentially compromised system configurations that could introduce subtle consistency violations or operational instabilities.

### Phase 9: Operational Monitoring and Recovery Completion

Recovery's final responsibility involves establishing [comprehensive monitoring infrastructure](../quick-reads/recovery/monitoring.md) for all transaction system components. This monitoring framework implements Bedrock's core fail-fast philosophy—rather than attempting complex error recovery procedures when components fail during normal operation, any failure of critical transaction processing components triggers immediate director shutdown and coordinated recovery restart.

**Component Coverage Strategy**  
The monitoring infrastructure covers sequencer, commit proxies, resolvers, logs, and the director itself with continuous operational validation. Storage servers face deliberate exclusion from this monitoring framework because they handle failures independently through sophisticated data distribution and repair mechanisms built into their operational design.

Transaction processing components, however, must maintain strict consistency guarantees under all operational conditions. Any failure within these critical components necessitates complete system recovery to preserve data integrity and consistency promises made to applications.

**Operational Mode Transition**  
Once monitoring becomes fully active and operational, the director executes its final transition from recovery mode to normal operational mode. The recovery attempt receives official completion marking, and the cluster begins accepting normal transaction processing requests from applications. The complete transformation from system failure state to trusted operational infrastructure is now complete.

## Recovery Architecture: From Crisis to Confidence

What commenced as a system in complete crisis—crashed components, uncertain data integrity, unclear operational state—has been systematically transformed into infrastructure that operators can trust completely. Recovery avoided attempting targeted repairs for immediate problems; instead, it methodically rebuilt everything from verified foundations, creating a system where every component has undergone validation, every piece of data has been accounted for, and every coordination relationship has been established cleanly.

This comprehensive reconstruction delivers something that targeted repair approaches struggle to provide: absolute confidence in system state integrity. When monitoring dashboards display green status indicators and transaction processing resumes normal operation, the objective is eliminating lingering questions about hidden corruption, incomplete fixes, or subtle consistency violations. The successful system transaction that stored the new configuration provides definitive proof that every component in the pipeline can coordinate correctly—sequencer, commit proxies, resolvers, logs, and storage all operating in designed harmony.

**Data Preservation Guarantee**  
Crucially, all recoverable data survived the reconstruction process intact. The conservative approach to version determination combined with careful preservation of committed transactions ensures that despite infrastructure failures, no committed application work faces loss. Applications can resume operation from precisely where the system maintained high confidence in data durability, with no gaps in their transactional history.

**Foundation for Reliable Operation**  
The cluster now stands ready to serve applications with complete confidence, supported by clean architectural foundations. Storage servers possess precise knowledge of their data responsibilities, logs contain verified transaction histories, and the monitoring system oversees components that were constructed to work together rather than patched to coexist after failure.

This represents the fundamental value proposition that justifies recovery time investment—trading brief operational downtime for a system constructed from verified components with transparent operational state. The result is infrastructure built for reliability rather than mere functionality, providing the foundation for sustained, trustworthy distributed system operation.

---

[^1]: FoundationDB's recovery approach is detailed in ["FoundationDB: A Distributed Unbundled Transactional Key Value Store"](https://www.foundationdb.org/files/fdb-paper.pdf) and implemented in Bedrock at `lib/bedrock/control_plane/director/recovery.ex`
