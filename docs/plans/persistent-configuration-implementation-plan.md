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

### Phase 2: Coordinator Bootstrap ✅ COMPLETED
- [x] Modify `coordinator/server.ex` init to read from storage first
- [x] Add system config reading using `Storage.fetch/3`
- [x] Handle BERT deserialization errors gracefully
- [x] Test bootstrap with and without existing storage
- [x] Verify Raft initialization with storage version
- [x] Refactor bootstrap logic into separate `impl.ex` module for better testability
- [x] Create comprehensive unit tests for bootstrap functionality

**📖 Reference**: [Persistent Configuration - Bootstrap Flow](.clinerules/01-architecture/persistent-configuration.md#bootstrap-flow)

### Phase 3: Director System Transaction ✅ COMPLETED
- [x] **CRITICAL**: Verify `CommitProxy.commit/2` transaction format requirements (tuple format confirmed)
- [x] Update documentation to match actual tuple-based transaction format
- [x] Add config persistence module with PID → {otp_name, node} encoding/decoding
- [x] Add system transaction building in director recovery completion
- [x] Add explicit readiness check before system transaction (via `:persist_system_state` state)
- [x] Implement fail-fast on system transaction failure (director exit)
- [x] Add telemetry events for system transaction monitoring
- [x] **TESTING**: Test system transaction submission and error handling

**📖 Reference**: [Control Plane Components - Director Persistence](.clinerules/03-implementation/control-plane-components.md#director-system-state-persistence)

### Phase 4: Integration & Self-Healing ✅ COMPLETED
- [x] Add director monitoring in coordinator
- [x] Add automatic director restart on failure detection
- [x] **COMPLETED**: Simplify to "let it crash" approach (removed circuit breaker complexity)
- [x] **COMPLETED**: Add component monitoring in director (monitor all transaction components)
- [x] **COMPLETED**: Implement immediate director exit on any component failure
- [x] Test simplified recovery retry cycle
- [x] Test edge cases (partial failures, network issues, corrupted data)
- [x] Verify system converges to stable state with fast recovery cycles

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
