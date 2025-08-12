# Resolver Startup

**Starting MVCC conflict detection components to prevent transaction conflicts.**

The resolver startup phase converts abstract [resolver](../../deep-dives/architecture/data-plane/resolver.md) descriptors from [vacancy creation](vacancy-creation.md) into operational processes that implement [Multi-Version Concurrency Control (MVCC)](../../glossary.md#multi-version-concurrency-control) conflict detection across the distributed system.

## What Happens

Recovery transforms resolver placeholders into working components through three key operations:

**Keyspace Assignment**: Resolvers receive specific [key ranges](../../glossary.md#key-range) with complete coverage—every possible key has exactly one responsible resolver for conflict detection.

**Log Assignment**: Each resolver gets assigned the minimal set of transaction logs containing data relevant to their key ranges, using tag-based filtering to avoid unnecessary data transfer.

**Historical State Recovery**: Resolvers rebuild their complete MVCC state by processing historical transactions within the recovery [version](../../glossary.md#version) range, enabling them to detect conflicts between pre-recovery and post-recovery transactions.

## Process Distribution

Recovery distributes resolver processes across resolution-capable nodes using round-robin assignment for fault tolerance. Each resolver receives a lock token and immediately begins recovering its conflict detection state from assigned transaction logs.

## Critical Requirements

Resolvers must rebuild complete historical state to maintain MVCC consistency across the recovery boundary. Without this, the system could not detect conflicts between old and new transactions, potentially allowing data corruption.

Recovery fails fast if resolution-capable nodes become unavailable or individual resolver startup fails—transaction isolation guarantees cannot be maintained without complete conflict detection coverage.

## Implementation

**Source**: `lib/bedrock/control_plane/director/recovery/resolver_startup_phase.ex`

**Key Inputs**: Resolver descriptors, storage team assignments, transaction logs, version vector bounds

**Output**: Operational resolver processes ready for MVCC conflict detection

## Next Phase

Recovery proceeds to [Transaction System Layout](transaction-system-layout.md), where operational resolvers join the coordination blueprint for distributed transaction processing.
