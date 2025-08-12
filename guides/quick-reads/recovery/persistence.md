# Persistence

**Validating the rebuilt system by storing its configuration through a complete transaction.**

The [Transaction System Layout](transaction-system-layout.md) provides the blueprint for component coordination, but blueprints remain theoretical until proven. The persistence phase bridges this gap by executing a **system transaction** that stores the new configuration while simultaneously validating that all components can work together as an integrated system.

## System Transaction Validation

Instead of running separate tests, Bedrock uses an elegant approach: the new system stores its own configuration. This system transaction exercises every component in the transaction pipeline:

- **[Sequencer](../../components/data-plane/sequencer.md)** assigns the transaction a [version](../../glossary.md#version) number
- **[Commit proxies](../../components/data-plane/commit-proxy.md)** coordinate submission
- **[Resolvers](../../components/data-plane/resolver.md)** perform conflict detection
- **[Logs](../../components/data-plane/log.md)** ensure durable persistence
- **[Storage](../../components/data-plane/storage.md)** servers commit the data

Successful completion proves the system can process real transactions with full [ACID](../../glossary.md#acid) guarantees, not just exist as isolated components.

## Dual Storage Format

The configuration gets stored in two complementary formats:

**Consolidated**: Complete layout under a single key for atomic retrieval during coordinator handoffs and architectural analysis.

**Decomposed**: Individual component configurations under separate keys for efficient targeted access during normal operations.

This approach optimizes both system-wide operations and component-specific queries while maintaining data consistency.

## Fail-Fast Error Handling

If the system transaction fails for any reason, the [director](../../glossary.md#director) terminates immediately rather than attempting repairs. This reflects a critical insight: transaction failure indicates the integrated system cannot coordinate correctly to deliver transactional guarantees.

Rather than risk deploying compromised infrastructure, recovery exits cleanly and enables restart with a fresh [epoch](../../glossary.md#epoch).

## Configuration Foundation

Beyond validation, persistence establishes the durable configuration foundation for all future recovery operations. The stored layout becomes the authoritative record for subsequent recovery attempts to understand existing infrastructure, data preservation requirements, and reconstruction needs.

## Recovery Completion

Successful system transaction completion transforms the cluster from theoretical components into a proven, operational distributed database. With infrastructure reconstructed, data preserved, coordination validated, and configuration stored, only [monitoring](monitoring.md) setup remains to complete recovery.

**Implementation**: `lib/bedrock/control_plane/director/recovery/persistence_phase.ex`

**Previous**: [Transaction System Layout](transaction-system-layout.md) | **Next**: [Monitoring](monitoring.md)
