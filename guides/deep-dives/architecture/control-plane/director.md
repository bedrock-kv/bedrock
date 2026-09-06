# Director

The [Director](../../../glossary.md#director) is Bedrock's [recovery](../../../glossary.md#recovery) orchestrator, serving as the singular authority for rebuilding the distributed [transaction](../../../glossary.md#transaction) system after failures. Created by the [Coordinator](../../../glossary.md#coordinator), it manages multi-phase service restoration, dependency sequencing, and ensures only one recovery process operates through [epoch](../../../glossary.md#epoch)-based authority.

**Location**: [`lib/bedrock/control_plane/director.ex`](../../../lib/bedrock/control_plane/director.ex)

## Recovery Authority

Each Director receives the coordinator's durable bootstrap reservation. Its exact `{generation, recovery_id}` pair determines recovery authority. Higher generations win; the same pair is an idempotent retry; a different id at the same generation is a competing owner and fails closed. The Director validates that the reservation generation equals its epoch before contacting any durable worker.

When Coordinator leadership changes during recovery, the new Coordinator creates a Director with a higher epoch, automatically superseding existing recovery processes.

## Recovery Orchestration

Director executes multi-phase recovery addressing component dependencies. See the **[Overview](../../../quick-reads/recovery.md)** or the **[Deep Dive](../../../deep-dives/recovery.md)**.

## Service Lifecycle Management

Director manages service creation and replacement through the use of local [Foreman](../../../glossary.md#foreman) on participating nodes. Placement decisions balance fault tolerance, network topology, and node capabilities to maximize reliability while minimizing recovery time.

Health monitoring tracks functional capability of each component, ensuring services can perform their designated transaction system roles.

## Related Components

- **[Coordinator](coordinator.md)**: Creates and manages Director lifecycle with epoch assignment
- **[Foreman](../infrastructure/foreman.md)**: Infrastructure component that creates workers under Director coordination  
- **[Log](../data-plane/log.md)**: Critical data plane component requiring Director-managed recovery sequencing
- **[Materializer](../data-plane/materializer.md)**: Data plane component with complex dependency requirements during recovery
