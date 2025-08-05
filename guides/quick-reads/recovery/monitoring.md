# Recovery Monitoring

**The final recovery phase that establishes operational oversight and completes the transition to normal operation.**

Recovery monitoring is the last phase in Bedrock's systematic recovery process. After all components have been reconstructed and validated, the monitoring phase establishes process supervision for critical components and transitions the cluster from recovery mode to normal transaction processing.

## What Gets Monitored

The monitoring system supervises all critical transaction processing components that require [fail-fast recovery](../../glossary.md#fail-fast-recovery):

- **[Sequencer](../../components/control-plane/sequencer.md)** - Global version number authority
- **[Commit Proxies](../../components/control-plane/commit-proxy.md)** - Transaction coordination
- **[Resolvers](../../components/control-plane/resolver.md)** - [MVCC](../../glossary.md#multi-version-concurrency-control-mvcc) conflict detection  
- **[Logs](../../components/data-plane/log.md)** - Transaction durability
- **Director** - Recovery coordination and cluster health

[Storage](../../components/data-plane/storage.md) servers are deliberately excluded because they have independent failure handling through replication and automatic recovery mechanisms.

## Fail-Fast Philosophy

When any monitored component fails during normal operation, the [director](../../glossary.md#director) immediately exits and triggers a new recovery cycle. This approach prioritizes consistency over availability—brief downtime is acceptable, but operating with potentially corrupted state is not.

This design choice trades sophisticated error handling for operational simplicity and guaranteed correctness.

## Operational Transition

Once monitoring is established, the director completes several final steps:

1. **Recovery Completion** - Marks the recovery attempt as finished and cleans up temporary state
2. **System Authorization** - Enables the cluster to accept normal transaction requests
3. **Mode Switch** - All components transition from recovery to operational mode

## System Verification

The successful establishment of monitoring provides final verification that:

- All critical components are healthy and communicating
- The transaction pipeline operates end-to-end
- The persistent [Transaction System Layout](transaction-system-layout.md) provides durable coordination information

## Recovery Success

At this point, the cluster has been systematically rebuilt from verified foundations. All recoverable committed data has been preserved, and the system is ready for trusted distributed operation with complete operational transparency.

---

**Source**: `lib/bedrock/control_plane/director/recovery/monitoring_phase.ex`

**See Also:**
- [Recovery Overview](../recovery.md) - Complete recovery phase sequence
- [Transaction System Layout](transaction-system-layout.md) - Coordination blueprint construction  
- [Recovery Deep Dive](../../deep-dives/recovery.md) - Comprehensive recovery architecture and philosophy