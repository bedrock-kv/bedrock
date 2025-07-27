# Recovery Guide

**Comprehensive recovery patterns and troubleshooting for Bedrock's distributed system.**

See `Bedrock.ControlPlane.Director.Recovery` module documentation for complete implementation details.

## See Also
- `lib/bedrock/control_plane/director/recovery.ex` - Main recovery orchestrator
- `lib/bedrock/control_plane/director/recovery/` - Individual phase implementations  
- `lib/bedrock/control_plane/config/recovery_attempt.ex` - Recovery state management

## Recovery Overview

See `Bedrock.ControlPlane.Director.Recovery` moduledoc for comprehensive recovery philosophy, state machine, and implementation patterns.

### Key Concepts
- **Fail-Fast Philosophy**: Combines Erlang/OTP "let it crash" with FoundationDB fast recovery
- **Epoch-Based Management**: Prevents split-brain with generation counters
- **Linear State Machine**: 17-phase recovery process from service locking to completion
- **Component Monitoring**: Any transaction component failure triggers full recovery

## When Recovery Triggers

### Critical Components (Recovery Triggers)
Recovery is triggered when any of these components fail:
- **Coordinator**: Raft consensus failure or network partition
- **Director**: Recovery coordinator failure
- **Sequencer**: Version assignment failure
- **Commit Proxies**: Transaction batching failure
- **Resolvers**: Conflict detection failure
- **Transaction Logs**: Durability system failure

### Non-Critical Components (No Recovery)
These failures do NOT trigger recovery:
- **Storage Servers**: Data distributor handles storage failures
- **Gateways**: Client interface failures are handled locally
- **Rate Keeper**: Independent component with separate lifecycle

### Detection Mechanisms
- **Coordinator Failure**: Raft heartbeat timeout → Leader election
- **Director Failure**: `Process.monitor/1` → Coordinator restart with incremented epoch
- **Component Failure**: Director monitors ALL transaction components → ANY failure → Director immediate exit

### Node Rejoin Triggers Recovery
Recovery can  also be triggered when nodes attempt to rejoin the cluster:
- **Node Rejoin**: When a node restarts and advertises services → Director restarts recovery
- **Reason**: Director needs up-to-date view of all available services for optimal layout
- **Correct Behavior**: Recovery restart on rejoin follows "simple flowchart" philosophy
- **Not a Bug**: Preventing recovery restart would add complex edge case handling

## Durable Services and Epoch Management

### Durable Service Nature
Logs and storage servers are **durable by design**:
- **Purpose**: Persist data across cold starts and node restarts
- **Lifecycle**: Survive node restarts, director failures, and epoch changes
- **Local Startup**: When node boots, it starts locally available durable services
- **Cluster Integration**: Services advertise to cluster via `request_to_rejoin`

### Epoch-Based Split-Brain Prevention
Durable services use epoch management to prevent split-brain scenarios:
- **Service Locking**: Director locks services with new epoch during recovery
- **Old Epoch Services**: Services with older epochs stop participating
- **New Epoch Services**: Only services locked with current epoch participate
- **Fail-Safe**: Services refuse commands from directors with older epochs

### Node Rejoin Flow
1. **Node Boots**: Starts locally available durable services (logs/storage)
2. **Service Advertisement**: Node reports existing services via `request_to_rejoin`
3. **Recovery Restart**: Director restarts recovery to incorporate new services
4. **Service Locking**: Director locks advertised services with new epoch
5. **Recovery Proceeds**: Director incorporates locked services into new layout
6. **Epoch Validation**: Old services terminate, new services participate

## Recovery Process

### Phase 1: Coordinator Election
When coordinator fails:
1. Remaining coordinators detect failure via Raft heartbeat timeout
2. Raft leader election selects new coordinator
3. New coordinator reads persistent configuration from storage
4. Coordinator initializes with highest epoch from storage

### Phase 2: Director Startup
When director fails or new coordinator elected:
1. Coordinator starts new director with incremented epoch
2. Director monitors coordinator for newer epoch announcements
3. Old director (if any) detects newer epoch and exits immediately
4. New director begins recovery process

### Phase 3: Service Discovery and Locking
1. Director discovers available services via foreman
2. Director locks available services with current epoch
3. Services with older epochs terminate themselves
4. Director collects service capabilities and status

### Phase 4: Transaction System Recovery
1. **Determine Durable Version**: Find highest committed version across logs
2. **Create Service Layout**: Assign roles based on available services
3. **Start Core Services**: Launch sequencer, commit proxies, resolvers
4. **Initialize Logs**: Create or recover transaction logs
5. **Replay Transactions**: Ensure all committed transactions are applied

### Phase 5: System Validation
1. **System Transaction**: Submit transaction that tests entire pipeline
2. **Persistence**: Store cluster configuration in system keyspace
3. **Component Monitoring**: Begin monitoring all transaction components
4. **Ready State**: Mark system as operational

### Phase 6: Continuous Monitoring
1. **Component Monitoring**: Monitor all transaction system components
2. **Failure Detection**: Any component failure triggers immediate director exit
3. **Epoch Management**: Increment epoch on each recovery attempt
4. **Self-Healing**: Coordinator automatically restarts failed director

### Key Implementation Points
- **Director monitors ALL transaction components**
- **ANY component failure → Director immediate exit**
- **Coordinator uses simple exponential backoff**
- **No circuit breaker complexity**
- **Epoch-based generation management**
