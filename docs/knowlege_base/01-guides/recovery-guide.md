# Understanding Bedrock's Recovery: Why Rebuilding Beats Repairing

**How a distributed database achieves reliability by choosing simplicity over optimization.**

Imagine your house foundation starts cracking. You could patch each crack individually, reinforce weak spots, and monitor for new problems—but you'd never be completely confident the foundation won't fail again. Or you could rebuild the entire foundation from scratch, ensuring every part meets current standards.

Bedrock chooses to rebuild. When any critical component fails, instead of trying to repair the specific problem, Bedrock rebuilds the entire [transaction processing system](../../bedrock-architecture.md) from a known-good state. This document explains why this approach works and how it delivers better reliability than attempting to optimize individual failure scenarios.

## The Core Insight: Worst Case as Best Case

Most distributed systems are optimized for the common case—normal operation when everything works correctly. They implement elaborate failure detection, partial recovery mechanisms, and complex repair procedures for specific failure modes. Each mechanism introduces new failure modes, creating an intricate web of interdependent recovery logic that becomes increasingly difficult to reason about, test, and trust.

Bedrock takes a fundamentally different approach, inspired by FoundationDB's architecture[^1]: optimize for the worst case scenario. When anything goes wrong, assume everything might be wrong and rebuild the entire transaction system. This trades recovery time—which we expect to be measured in seconds in most cases—for system simplicity and predictability.

The theory of reliability supports this choice. A distributed database should spend the vast majority of its time in steady state, not recovering. Optimizing for normal operation while keeping recovery simple should yield better overall reliability than trying to optimize recovery speed at the cost of complexity. After a failure, operators need confidence that the system is truly healthy, and a comprehensive rebuild should provide that confidence in a way that targeted repairs cannot.

Perhaps most importantly, complex recovery logic is extraordinarily difficult to test. You cannot easily simulate every possible combination of partial failures, network partitions, and timing issues. Simple, comprehensive recovery should be more testable because there's fundamentally only one path through the system, though real-world deployment will ultimately prove this approach.

## The Recovery Journey: From Chaos to Order

Understanding Bedrock's recovery requires following the complete journey from a potentially chaotic failure state to a fully operational, trusted system. Each phase builds upon the previous one, creating a logical progression that transforms uncertainty into confidence.

### Establishing Control: The Service Locking Foundation

Recovery begins with a fundamental challenge: when the system has failed and multiple directors might attempt recovery simultaneously, who's in charge? The answer lies in Bedrock's [service locking mechanism](../02-deep/recovery/service-locking.md), which establishes exclusive control over the remnants of the failed system.

Think of service locking as a coordinated evacuation before rebuilding. When a building shows structural damage, you don't immediately tear it down—first, you evacuate the occupants safely and establish a perimeter to prevent unauthorized entry. Similarly, recovery locks services from the old transaction system, instructing them to halt all normal operations and report exclusively to the recovery director.

This locking protocol serves three critical purposes beyond simple coordination. First, it prevents the [split-brain problem](../../glossary.md#split-brain) that plagues distributed systems—each service accepts locks from only one director at a time, with epoch-based precedence ensuring that newer recovery attempts supersede older ones. Second, it immediately halts all transaction processing, preventing further data corruption or inconsistency while recovery proceeds. Third, it validates that services are genuinely operational and reachable, returning crucial state information including current transaction versions and durability status.

The locking process reveals an important design principle: only services that were part of the previous transaction system layout become candidates for locking. These services contain committed transaction data that must be preserved during recovery. Other available services in the cluster remain unlocked and free for recruitment into the new system architecture.

Recovery attempts to lock all old services in parallel, but individual failures are expected and handled gracefully. The system may have failed due to partial component failures, so recovery gathers and locks as many services as possible, determining in subsequent phases what can be salvaged. However, if any service reports that it's already locked by a director with a newer epoch, recovery immediately halts—this indicates that another, more recent recovery attempt has superseded this one.

### Understanding the Starting Point: New Cluster or Data Recovery

With control established, recovery faces a fundamental branching decision that determines the entire subsequent path: is this a new cluster being initialized for the first time, or an existing cluster that needs to recover from a failure state?

The determination is elegantly simple. If no logs existed in the previous transaction system layout, this represents a completely new cluster deployment. Recovery transitions to [initialization mode](../02-deep/recovery/path-determination.md), creating the foundational structure for a fresh distributed database. This path creates initial log placeholders and establishes the basic keyspace division between user data and system metadata, setting up the scaffolding for normal transaction processing.

If logs were defined in the previous layout—regardless of whether they're currently reachable—this was a running system with potentially valuable data. Recovery must now embark on the more complex path of data preservation and system reconstruction. This automatic path selection eliminates the need for external cluster state detection mechanisms while ensuring that existing systems are never mistaken for new deployments, preventing accidental data loss.

### The Data Detective Work: Log Recovery Planning

When recovering an existing cluster, Bedrock faces one of the most intellectually challenging problems in distributed systems: determining what committed transaction data can be safely recovered from a potentially failed infrastructure. The solution lies in understanding how Bedrock's transaction architecture creates natural redundancy.

During normal operation, Bedrock employs a crucial design principle: every committed [transaction](../00-quick/transaction-quick.md) is sent to every log in the system. This means that each [log](../../components/log.md) contains a complete record of the transaction history, creating multiple authoritative sources for recovery. However, log failures might be precisely why recovery became necessary, so not all logs can be guaranteed available or intact.

The [log recovery planning algorithm](../02-deep/recovery/log-recovery-planning.md) approaches this challenge systematically. Bedrock organizes logs into shards for parallel processing, and recovery must ensure that each shard can provide sufficient data to guarantee consistency. The algorithm examines available logs within each shard, generates all possible combinations that meet quorum requirements, and calculates the recoverable version range for each combination.

Consider a concrete example: imagine a system with six logs distributed across two shards, where each shard requires two logs for quorum. Shard A contains logs 1, 2, and 3, while Shard B contains logs 4, 5, and 6. If logs 1, 2, 4, and 5 are available with complete transaction data through version 100, but logs 3 and 6 are unreachable, recovery can safely recover through version 100 because both shards achieve their quorum requirements.

The final recovery decision represents the cross-shard minimum of these ranges—recovery includes only the highest transaction version that's guaranteed complete across every participating shard. This conservative approach is designed to ensure that the recovered system maintains strict consistency guarantees while maximizing the amount of transaction history preserved for the new generation.

### Planning the New Architecture: The Vacancy Abstraction

Recovery now confronts a sophisticated planning challenge: how do you specify the architecture of a completely new distributed system without prematurely committing to specific machines or processes? The answer lies in one of Bedrock's most elegant abstractions: [vacancy placeholders](../02-deep/recovery/vacancy-creation.md).

Instead of immediately assigning concrete services to roles, recovery creates numbered vacancy slots like `{:vacancy, 1}`, `{:vacancy, 2}`, and `{:vacancy, 3}` that represent service requirements without binding them to actual infrastructure. This separation of concerns proves brilliant in practice—the planning phase answers "what do we need?" while the subsequent recruitment phase answers "which services provide it?"

The vacancy creation process treats different service types according to their fundamental characteristics. Logs contain no persistent state beyond what can be recreated by replaying transactions from other logs, so recovery employs a clean-slate approach. If the old system had logs serving specific shard combinations and the configuration specifies three logs per shard, recovery creates exactly three log vacancies for each shard combination, regardless of how many old logs are available.

Storage services present a fundamentally different challenge because they contain persistent data that's expensive and time-consuming to migrate. Recovery takes an ultra-conservative approach with [storage](../../components/storage.md), preserving all existing storage assignments and creating additional vacancy slots only where the configuration demands higher replication factors. If a storage team currently has two replicas but the configuration requires three, recovery creates exactly one storage vacancy for that team.

[Resolvers](../../components/resolver.md), the components responsible for transaction conflict detection, receive special treatment during vacancy creation. Since resolvers are purely computational components that maintain MVCC state derived from transaction logs, recovery creates resolver descriptors that map each storage team's key range to a resolver vacancy. These descriptors define the boundaries between resolvers without committing to specific resolver processes, allowing the subsequent startup phase to optimize resolver placement and assignment.

### Establishing the Recovery Baseline: Version Determination

Before beginning reconstruction, recovery must establish a critical reference point: what's the highest transaction version that can be guaranteed durable across the entire cluster? This [version determination process](../02-deep/recovery/version-determination.md) represents one of the most mathematically sophisticated aspects of recovery, requiring careful analysis of storage team health and replica availability.

The fundamental challenge stems from the reality of distributed system operation. Storage servers inevitably operate at slightly different transaction versions due to processing lag, network delays, or partial failures. Some replicas might be current with the latest transactions, while others lag behind by various amounts. Recovery must identify a conservative baseline where sufficient replicas have durably committed the data, ensuring that the recovered system can provide consistent access to all transactions at or below this version.

The algorithm operates by calculating a fault-tolerant version within each storage team, then taking the minimum across all teams to establish the cluster-wide recovery baseline. For each storage team, recovery examines the durable versions reported by available replicas, sorts them in ascending order, and selects the version that ensures quorum availability. In a three-replica team with replicas at versions [95, 98, 100], if the quorum requirement is two replicas, the team's durable version becomes 98—the second-highest version that guarantees two replicas possess the data even if the most current replica fails.

Once each team's durable version is calculated, the cluster-wide durable version emerges as the minimum across all teams. This conservative approach ensures that every piece of data in the cluster is available at the recovery baseline, even accounting for potential replica failures during the new system's startup process. Teams are simultaneously classified as healthy, degraded, or insufficient based on their replica counts relative to the desired replication factor, providing valuable information for subsequent rebalancing operations.

### Building the New Infrastructure: Service Recruitment

With the architectural plan complete, recovery transitions from abstract planning to concrete infrastructure construction. The recruitment phases transform vacancy placeholders into actual running services, but the process differs significantly between service types based on their operational characteristics and data preservation requirements.

#### Log Recruitment: Embracing Fresh Starts

[Log recruitment](../02-deep/recovery/log-recruitment.md) embodies Bedrock's philosophy of choosing reliability over optimization. Since logs contain no persistent state beyond transaction records that exist in other logs, recovery can be aggressive about creating entirely new log infrastructure rather than attempting to salvage potentially problematic old components.

The recruitment process follows a three-phase strategy designed to balance efficiency with safety. Recovery first identifies existing log services that are available in the cluster but weren't part of the previous transaction system. These services represent ideal candidates because they already exist as running processes but carry no potentially corrupted data from the failed system.

When existing services prove insufficient to fill all vacancy positions, recovery creates new log workers and distributes them across available nodes using a round-robin assignment strategy. This distribution approach ensures fault tolerance by spreading critical components across different machines rather than concentrating them on adjacent nodes that might share failure modes.

The recruitment process culminates with a critical locking step that establishes exclusive control over all recruited services. This locking prevents interference from other processes or competing recovery attempts and validates that the recruited services are genuinely operational and responsive.

#### Storage Recruitment: The Ultra-Conservative Approach

[Storage recruitment](../02-deep/recovery/storage-recruitment.md) represents the most cautious aspect of recovery because storage services contain the actual committed data—user records, system state, and historical information that required significant computation to generate and would be expensive or impossible to recreate if lost.

The recruitment algorithm implements a strict preservation hierarchy that prioritizes data safety above all other concerns. Recovery never touches services from the old transaction system during recruitment, ensuring that their valuable data remains undisturbed and available for potential future recovery attempts. Only services that are available in the cluster but weren't part of the previous system become candidates for recruitment.

The assignment process begins by counting all storage vacancies across all teams—these represent the gaps between current replica counts and desired replication factors. Recovery then identifies suitable candidate services and assigns them to fill vacancies directly. When candidates prove insufficient, recovery calculates the number of new storage workers required and creates them using the same fault-tolerant distribution strategy employed for logs.

As with log recruitment, the process concludes with comprehensive locking of all recruited services to establish exclusive control and validate operational status. This ultra-conservative approach ensures that valuable persistent data is never accidentally destroyed during the recovery process.

### Moving the Data: Log Replay and System Integration

With new infrastructure in place, recovery faces the crucial challenge of transferring committed transaction data from the old system to the new one. The [log replay phase](../02-deep/recovery/log-replay.md) handles this data migration while simultaneously validating the operational integrity of the new log infrastructure.

Bedrock's approach to log replay reflects its core philosophy of choosing reliability over optimization. Even though old logs contain the correct transaction data, recovery copies this data to the newly recruited logs rather than attempting to reuse the old storage. This duplication ensures that all essential transaction data resides on verified, reliable storage before the new system begins operation.

The replay algorithm pairs each new log with source logs from the old system using a round-robin assignment strategy. If the new system contains more logs than the old one—a common scenario when scaling up—the algorithm cycles through the old logs repeatedly to ensure balanced distribution. A system transitioning from two old logs to four new logs would create pairings like: new log 1 copies from old log 1, new log 2 copies from old log 2, new log 3 copies from old log 1, and new log 4 copies from old log 2.

Only committed transactions within the established version vector are copied during replay. Uncommitted transactions are deliberately discarded since they were never guaranteed durable and their inclusion could compromise system consistency. This selective copying ensures that the new system begins operation with a clean, consistent transaction history.

### Starting the Transaction Engine: Component Coordination

With data properly positioned, recovery begins the complex process of starting the various components that enable distributed transaction processing. Each component serves a specific role in the transaction pipeline, and their startup must be carefully orchestrated to ensure proper coordination and avoid race conditions.

#### The Sequencer: Establishing Global Order

The [sequencer startup phase](../02-deep/recovery/sequencer-startup.md) addresses one of the fundamental challenges in distributed systems: how do you ensure that every transaction receives a unique, totally-ordered version number that all components agree upon? The solution lies in Bedrock's [sequencer](../../components/sequencer.md) component—a singleton service that provides the authoritative source of transaction version numbers across the entire cluster.

Global ordering requires a single authority to prevent version conflicts that would compromise transaction consistency. The sequencer starts on the director's current node using the last committed version from the recovery baseline as its starting point. From this foundation, the sequencer will assign version numbers incrementally for new transactions, ensuring no gaps or overlaps in the version sequence.

The sequencer's role extends beyond simple number assignment. It coordinates with [commit proxies](../../components/commit-proxy.md) to ensure that transaction versions are assigned consistently across the distributed system, and it provides the timing foundation that enables other components to maintain their state correctly.

#### Proxy Startup: Enabling Horizontal Scalability

The [proxy startup phase](../02-deep/recovery/proxy-startup.md) establishes the [commit proxy](../../components/commit-proxy.md) components that provide horizontal scalability for transaction processing. Multiple proxies can handle increasing transaction volume without creating bottlenecks at individual components, and their distributed nature ensures that the system can continue processing transactions even if individual proxies fail.

Recovery uses round-robin distribution to start the desired number of commit proxy processes across nodes with coordination capabilities. This geographical distribution ensures fault tolerance by spreading critical transaction processing components across different machines rather than concentrating them in a single location.

Each proxy receives configuration information including the current epoch and director details, enabling them to detect epoch changes and coordinate properly with the recovery system. Proxies remain locked during startup until the system layout phase transitions them to operational mode, preventing premature transaction processing before the complete system is ready.

#### Resolver Startup: Implementing Conflict Detection

The [resolver startup phase](../02-deep/recovery/resolver-startup.md) handles one of the most sophisticated aspects of distributed transaction processing: how do you prevent conflicting transactions from corrupting data when multiple clients modify overlapping key ranges simultaneously?

The solution lies in Bedrock's resolver components, which implement Multi-Version Concurrency Control (MVCC) conflict detection. Each resolver handles a specific key range and maintains detailed conflict detection state that tracks which transactions have accessed which keys at which versions.

The startup process transforms the abstract resolver descriptors created during vacancy planning into operational resolver processes. This transformation requires complex range-to-storage-team mapping and tag-based log assignment to ensure that each resolver receives precisely the data it needs for conflict detection.

Resolvers must recover their historical MVCC state by processing transactions from assigned logs within the established version vector range. This recovery builds the conflict detection information needed to properly evaluate future transaction conflicts, ensuring that the MVCC guarantees remain intact across the recovery boundary.

### Making It Official: System Layout and Persistence

All components are now running, but they exist in isolation—the sequencer doesn't know which logs to notify about new versions, commit proxies don't know which resolvers handle conflict detection for specific key ranges, and storage servers don't know which logs contain the transactions they need. Recovery solves this coordination challenge by constructing and persisting the [Transaction System Layout](../02-deep/recovery/transaction-system-layout.md).

The [Transaction System Layout](../../transaction-system-layout.md) serves as the comprehensive blueprint that enables distributed system components to find and communicate with each other. This layout contains the complete mapping of services to roles, the assignment of key ranges to storage teams, the association of resolvers with their responsibility areas, and all the coordination information necessary for distributed transaction processing.

Recovery validates that all core transaction components remain operational, then constructs the complete coordination blueprint. This validation is lightweight since previous phases already established communication with all components—it primarily confirms that nothing failed between startup and layout construction.

The layout construction culminates in a complete system transaction that stores the new configuration in the distributed database itself. This persistence step serves dual purposes: it durably stores the new system configuration for future reference, and it simultaneously validates that the entire transaction processing pipeline functions correctly. The system transaction exercises the complete data path from client request through proxy coordination, resolver conflict detection, sequencer version assignment, log persistence, and storage commitment.

If this system transaction fails, something is fundamentally wrong with the new system configuration. Rather than attempting repairs, recovery exits immediately, allowing the coordinator to restart with a fresh epoch. This fail-fast approach prevents the deployment of potentially compromised system configurations.

### Operational Vigilance: Monitoring and Completion

Recovery's final responsibility involves establishing [comprehensive monitoring](../02-deep/recovery/monitoring.md) for all transaction system components. This monitoring implements Bedrock's fail-fast philosophy—rather than attempting complex error recovery when components fail during normal operation, any failure of critical transaction processing components triggers immediate director shutdown and recovery restart.

The monitoring setup covers sequencer, commit proxies, resolvers, logs, and the director itself. Storage servers are notably excluded from this monitoring because they handle failures independently through data distribution and repair mechanisms. Transaction processing components, however, must maintain strict consistency, so any failure requires complete recovery.

Once monitoring becomes active, the director transitions from recovery mode to operational mode. The recovery attempt is marked as complete, and the cluster begins accepting normal transaction processing requests. The transformation from chaos to order is complete.

## The Journey's End: Order from Chaos

What began as a system in crisis—crashed components, uncertain data integrity, unclear system state—has become something operators should be able to trust. Recovery didn't just attempt to fix the immediate problems; it rebuilt everything from the ground up, creating a system where every component has been verified, every piece of data accounted for, and every coordination relationship established cleanly.

This comprehensive rebuild aims to deliver something that targeted repairs struggle to provide: high confidence in system state. When the monitoring dashboards turn green and transaction processing resumes, the goal is to minimize lingering questions about hidden corruption, incomplete fixes, or subtle inconsistencies. The successful system transaction that stored the new configuration demonstrates that every component in the pipeline can work together correctly—sequencer, proxies, resolvers, logs, and storage all coordinating as designed.

Crucially, all the recoverable data survived. The conservative approach to version determination and the careful preservation of committed transactions means that despite infrastructure failures, no committed work should be lost. Applications can resume from where the system last had high confidence in data durability.

The cluster now stands ready to serve its applications again, with a clean foundation underneath. Storage servers know exactly what data they're responsible for, logs contain verified transaction histories, and the monitoring system watches over components that were built to work together rather than patched to coexist. This is the goal that makes the recovery time investment worthwhile—trading seconds of downtime for a system built from verified components with clear operational state.

---

**Want to explore further?**
- For detailed recovery phase implementations: [Recovery Deep Dive](../02-deep/recovery/)
- For complete architectural context: [Bedrock Architecture](../../bedrock-architecture.md)
- For transaction processing details: [Transaction Deep Dive](../02-deep/transactions-deep.md)
- For component implementation details: [Components Documentation](../../components/)
- For hands-on development: [Development Setup Guide](../00-getting-started/development-setup.md)

The next time you observe Bedrock recover from a failure in a matter of seconds, remember that those seconds purchase something far more valuable than rapid restoration: they buy complete confidence in a system built from first principles, with every component verified and operational, ready to serve your data with the reliability that only comes from comprehensive reconstruction.

---

[^1]: FoundationDB's recovery approach is detailed in ["FoundationDB: A Distributed Unbundled Transactional Key Value Store"](https://www.foundationdb.org/files/fdb-paper.pdf) and implemented in Bedrock at `lib/bedrock/control_plane/director/recovery.ex`