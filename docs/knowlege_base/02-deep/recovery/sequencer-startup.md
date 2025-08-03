# Sequencer Startup Phase

**Starting the global version number authority for distributed transaction ordering.**

How does a distributed system ensure that every transaction gets a unique, totally-ordered version number that all components agree on? The `SequencerStartupPhase` solves this fundamental ordering problem by starting the sequencer component—a singleton service that provides the authoritative source of transaction version numbers. Without this global ordering, transaction processing would collapse into chaos as different components assign conflicting version numbers to concurrent transactions.

## Singleton Architecture

The sequencer is a critical singleton component where only one instance runs cluster-wide to ensure consistent version assignment. Multiple sequencers would create version conflicts that would compromise transaction consistency and system reliability.

## Startup Process

The phase starts the sequencer process on the director's current node with the last committed version from the version vector as the starting point. The sequencer will assign version numbers incrementally from this baseline for new transactions, ensuring no gaps or overlaps in the version sequence.

This initialization with the recovery baseline ensures that:
- New transaction versions continue seamlessly from where the old system left off
- No version numbers are accidentally reused 
- The global ordering remains consistent across the recovery boundary

## Critical Failure Handling

The phase immediately halts recovery with a fatal error if the sequencer fails to start, since version assignment is fundamental to transaction processing. Unlike temporary resource shortages that can be retried, sequencer startup failure on the director's own node indicates serious system problems requiring immediate attention.

Possible causes of sequencer startup failure include:
- Node resource exhaustion
- OTP supervision tree problems
- Network configuration issues
- Process registration conflicts

## Global Coordination Role

Global version assignment requires this singleton sequencer initialized with the current version baseline to ensure transaction ordering consistency across the entire cluster. The sequencer coordinates with:

- **Commit Proxies**: To assign commit versions to transactions
- **Gateways**: To provide read versions for transaction snapshots  
- **Storage Servers**: To maintain version-based data organization
- **Logs**: To ensure proper transaction ordering

## Input Parameters

- `recovery_attempt.epoch` - Current recovery epoch for coordination
- `recovery_attempt.version_vector` - Version range with last committed version as baseline
- `recovery_attempt.cluster.otp_name(:sequencer)` - OTP name for service registration

## Output Results

- `recovery_attempt.sequencer` - PID of operational sequencer ready to assign transaction versions

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/sequencer_startup_phase.ex`

For detailed component behavior and version assignment algorithms, see [Sequencer documentation](../components/sequencer.md).

## Next Phase

Proceeds to [Proxy Startup](10-proxy-startup.md) to start commit proxy components for transaction processing scalability.