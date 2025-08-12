# Sequencer Startup

**Starting the global version authority that ensures transaction ordering consistency.**

The [Sequencer](../../components/data-plane/sequencer.md) serves as Bedrock's singular source of truth for [version](../../glossary.md#version) assignment across the entire cluster. During recovery, establishing this global version authority is critical because all transaction processing depends on having unique, monotonically increasing version numbers.

## Why a Single Sequencer?

Version assignment requires exactly one authority cluster-wide. Multiple sequencers would create irreconcilable conflicts—imagine two sequencers simultaneously assigning version 1001 to different transactions. This architectural constraint trades theoretical availability for practical consistency, eliminating complex consensus protocols while ensuring proper [MVCC](../../glossary.md#multi-version-concurrency-control) semantics.

## Startup Process

The sequencer starts on the [director's](../../glossary.md#director) node using the recovery baseline from [version determination](version-determination.md). This initialization preserves three critical properties:

- **Continuity**: New versions begin exactly where the old system left off
- **Uniqueness**: No previously assigned version numbers get reused  
- **Causality**: The [Lamport clock](../../glossary.md#lamport-clock) sequence remains intact across recovery

## Failure Handling

Sequencer startup failure triggers immediate recovery termination because version assignment is an absolute prerequisite for transaction processing. When the sequencer cannot start on the director's own node, it indicates fundamental problems:

- Resource exhaustion preventing initialization
- OTP supervision tree issues
- Network configuration or process registration conflicts

Rather than attempting repairs, recovery terminates immediately—better to restart with a clean [epoch](../../glossary.md#epoch) than deploy an unreliable version authority.

## Integration with Transaction Pipeline

Once operational, the sequencer coordinates with other components through its three version counters:

- [Commit proxies](../../components/data-plane/commit-proxy.md) request version assignments for transaction batches
- [Gateways](../../components/infrastructure/gateway.md) obtain read versions for consistent snapshots
- [Storage servers](../../components/data-plane/storage.md) organize MVCC data structures using the version timeline

This creates system-wide synchronization where all components operate from a shared understanding of transaction ordering.

## Next Phase

With version authority established, recovery continues to [Commit Proxy Startup](proxy-startup.md) to deploy the horizontally scalable commit processing components.

---
*Implementation: `lib/bedrock/control_plane/director/recovery/sequencer_startup_phase.ex`*
