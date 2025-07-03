# Persistent Configuration Implementation Plan

## Goal
Implement self-bootstrapping persistent cluster configuration using the system's own storage infrastructure.

**📖 Complete Design**: See [Persistent Configuration Architecture](.clinerules/01-architecture/persistent-configuration.md)

## Implementation Checklist

### Phase 1: Foundation - Storage Discovery ✅ COMPLETED
- [x] Add `Foreman.storage_workers/2` function to filter storage workers
- [x] Test storage worker identification via `Worker.info/2`
- [x] Verify foreman wait mechanism works reliably
- [x] Handle graceful fallback when no storage workers exist

**📖 Reference**: [Control Plane Components - Coordinator Bootstrap](.clinerules/03-implementation/control-plane-components.md#coordinator-bootstrap-with-persistent-configuration)

### Phase 2: Coordinator Bootstrap
- [ ] Modify `coordinator/server.ex` init to read from storage first
- [ ] Add system config reading using `Storage.fetch/3`
- [ ] Handle BERT deserialization errors gracefully
- [ ] Test bootstrap with and without existing storage
- [ ] Verify Raft initialization with storage version

**📖 Reference**: [Persistent Configuration - Bootstrap Flow](.clinerules/01-architecture/persistent-configuration.md#bootstrap-flow)

### Phase 3: Director System Transaction
- [ ] **CRITICAL**: Verify `CommitProxy.commit/2` transaction format requirements
- [ ] Add config sanitization (remove PIDs/refs before BERT encoding)
- [ ] Add system transaction building in director recovery completion
- [ ] Add explicit readiness check before system transaction
- [ ] Implement fail-fast on system transaction failure (director exit)
- [ ] Test system transaction submission and error handling

**📖 Reference**: [Control Plane Components - Director Persistence](.clinerules/03-implementation/control-plane-components.md#director-system-state-persistence)

### Phase 4: Integration & Self-Healing
- [ ] Add director monitoring in coordinator
- [ ] Add automatic director restart on failure detection
- [ ] Test full recovery retry cycle
- [ ] Test edge cases (partial failures, network issues, corrupted data)
- [ ] Verify system converges to stable state without infinite retries

**📖 Reference**: [Persistent Configuration - Error Handling](.clinerules/01-architecture/persistent-configuration.md#error-handling-and-edge-cases)

## Success Metrics
1. **Cold Start**: Empty cluster → recovery → persistence → ready
2. **Warm Start**: Existing storage → bootstrap → ready (no recovery needed)
3. **Failure Recovery**: Failed system transaction → director restart → eventual success
4. **Persistence**: System state survives full cluster restart

## Key Resources
- **📖 Architecture**: [Persistent Configuration](.clinerules/01-architecture/persistent-configuration.md)
- **📖 Implementation**: [Control Plane Components](.clinerules/03-implementation/control-plane-components.md)
- **📖 Debugging**: [Quick Reference](.clinerules/00-start-here/quick-reference.md)
- **📖 Testing**: [Testing Strategies](.clinerules/02-development/testing-strategies.md)
