# Recovery Internals

This document explains Bedrock's recovery system, which combines FoundationDB's proven recovery approach with Erlang/OTP's "let it crash" philosophy to create a robust, fast-recovering distributed system.

## See Also
- **Architecture Overview**: [FoundationDB Concepts](foundationdb-concepts.md) - Core architectural principles and component relationships
- **Persistent State**: [Persistent Configuration](persistent-configuration.md) - Self-bootstrapping cluster state design
- **Implementation Details**: [Control Plane Components](../03-implementation/control-plane-components.md) - Director and Coordinator implementation
- **Development Support**: [Best Practices](../02-development/best-practices.md) and [Debugging Strategies](../02-development/debugging-strategies.md)
- **Testing Approaches**: [Testing Strategies](../02-development/testing-strategies.md) - Recovery testing patterns

## Recovery Philosophy

Bedrock's recovery system is built on two foundational principles:

### 1. "Let It Crash" (Erlang/OTP)
- **Fast Failure Detection**: Use process monitoring rather than complex health checking
- **Immediate Failure Response**: Any critical component failure triggers immediate director shutdown
- **Supervision Tree Restart**: Let Erlang's supervision trees handle automatic restart
- **Fail-Fast Error Handling**: Prefer immediate failure over complex error recovery

### 2. Fast Recovery Over Complex Error Handling (FoundationDB)
- **Component Failure Triggers Full Recovery**: Any transaction system component failure causes complete recovery
- **Process Suicide**: Processes terminate themselves when they detect newer generations
- **Recovery Count Mechanism**: Each recovery increments an epoch counter for generation management
- **Simple Failure Detection**: Use heartbeats and process monitoring, not complex availability checking

## When Recovery Is Triggered

### Critical Component Failures
Recovery is triggered when any of these components fail:
- **Coordinator**: Raft consensus failure or network partition
- **Director**: Recovery coordinator failure
- **Sequencer**: Version assignment failure
- **Commit Proxies**: Transaction batching failure
- **Resolvers**: Conflict detection failure
- **Transaction Logs**: Durability system failure

### Non-Critical Component Failures
These failures do NOT trigger recovery:
- **Storage Servers**: Data distributor handles storage failures
- **Gateways**: Client interface failures are handled locally
- **Rate Keeper**: Independent component with separate lifecycle

### Detection Mechanisms

#### Coordinator Failure Detection
- Coordinators send heartbeats to each other via Raft
- Coordinator kills itself if it cannot reach majority of coordinators
- Remaining coordinators elect new leader when current coordinator fails

#### Director Failure Detection
- Coordinator monitors director process via `Process.monitor/1`
- Director exits immediately on any transaction system component failure
- Coordinator detects director exit and starts new director with incremented epoch

#### Component Failure Detection
- Director monitors ALL transaction system components via `Process.monitor/1`
- ANY component death message triggers immediate director exit
- No complex health checking or partial recovery attempts

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

## Erlang/OTP Integration

### Supervision Tree Structure
```
Cluster Supervisor
├── Coordinator (permanent restart)
├── Director (temporary restart, managed by coordinator)
│   ├── Sequencer (temporary restart)
│   ├── Commit Proxies (temporary restart)
│   ├── Resolvers (temporary restart)
│   └── Transaction Logs (temporary restart)
├── Foreman (permanent restart)
└── Storage Workers (permanent restart)
```

### Process Monitoring Pattern
```elixir
# Director monitors all transaction components
def monitor_component(component_pid) do
  monitor_ref = Process.monitor(component_pid)
  # Store monitor_ref for cleanup
  # Exit immediately on :DOWN message
end

# Handle component failure
def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
  Logger.error("Transaction component failed: #{inspect(reason)}")
  # Exit immediately - let coordinator restart us
  exit({:component_failure, reason})
end
```

### Restart Strategies
- **Coordinator**: `:permanent` - Always restart on failure
- **Director**: `:temporary` - Only restart when coordinator decides
- **Transaction Components**: `:temporary` - Only exist during director lifetime
- **Storage/Foreman**: `:permanent` - Independent lifecycle

## Error Handling Principles

### Fail-Fast Philosophy
```elixir
# GOOD: Fail immediately on error
def get_available_commit_proxy([]), do: exit(:no_commit_proxies)
def get_available_commit_proxy([proxy | _]), do: {:ok, proxy}

# BAD: Complex error handling
def get_available_commit_proxy(proxies) do
  # Complex availability checking, retry logic, etc.
end
```

### Simple vs Complex Error Handling
| Situation | Simple Approach | Complex Approach |
|-----------|----------------|------------------|
| Remote process check | Use first available, fail if none | Check if each process is alive |
| Component failure | Exit immediately | Try to recover or work around |
| Network partition | Fail fast, let recovery handle | Complex partition detection |
| Resource exhaustion | Exit with clear reason | Try to free resources |

### Recovery Count Mechanism
```elixir
# Each recovery increments epoch
new_epoch = current_epoch + 1

# Components check epoch and exit if outdated
def handle_info({:epoch_changed, new_epoch}, %{epoch: current_epoch} = state) 
    when new_epoch > current_epoch do
  Logger.info("Newer epoch detected, terminating")
  exit(:newer_epoch_exists)
end
```

## Debugging Recovery Issues

### Common Recovery Problems

#### Recovery Hangs
- **Symptom**: Recovery process stops progressing
- **Diagnosis**: Check which recovery phase is stuck
- **Common Causes**: Service discovery failure, resource exhaustion
- **Solution**: Check foreman health, available services

#### Infinite Recovery Loops
- **Symptom**: Recovery keeps restarting
- **Diagnosis**: Check director exit reasons
- **Common Causes**: Persistent system transaction failure
- **Solution**: Check commit proxy, resolver, log health

#### Split Brain Scenarios
- **Symptom**: Multiple directors think they're active
- **Diagnosis**: Check epoch numbers and coordinator state
- **Common Causes**: Network partition, coordinator failure
- **Solution**: Ensure Raft quorum, check network connectivity

### Debugging Commands
```elixir
# Check coordinator state
GenServer.call(:bedrock_coordinator, :get_state)

# Check director state
GenServer.call(:bedrock_director, :get_state)

# Check recovery progress
:telemetry.attach("recovery-debug", 
  [:bedrock, :director, :recovery, :*], 
  &IO.inspect/4, nil)

# Check system configuration
Storage.fetch(storage_worker, "\xff/system/config", :latest)
```

### Telemetry Events
```elixir
# Recovery lifecycle
[:bedrock, :director, :recovery, :started]
[:bedrock, :director, :recovery, :phase_completed]
[:bedrock, :director, :recovery, :completed]
[:bedrock, :director, :recovery, :failed]

# Component monitoring
[:bedrock, :director, :component, :monitored]
[:bedrock, :director, :component, :failed]

# Coordinator management
[:bedrock, :coordinator, :director, :started]
[:bedrock, :coordinator, :director, :failed]
[:bedrock, :coordinator, :director, :restarted]
```

## Performance Characteristics

### Recovery Time
- **Cold Start**: 5-15 seconds (depending on cluster size)
- **Warm Restart**: 1-5 seconds (with persistent configuration)
- **Component Failure**: Sub-second detection, 1-3 second restart

### Resource Usage
- **Memory**: Minimal overhead for monitoring
- **CPU**: Burst during recovery, minimal during operation
- **Network**: Recovery coordination traffic, then normal operation

### Scalability
- **Node Count**: Recovery time increases logarithmically with cluster size
- **Data Size**: Storage recovery time depends on transaction log size
- **Component Count**: Linear increase in monitoring overhead

## Comparison to FoundationDB

| Aspect | FoundationDB | Bedrock |
|--------|--------------|---------|
| Recovery Trigger | Component failure | Component failure |
| Failure Detection | Heartbeats + process monitoring | Process monitoring (Erlang) |
| Recovery Coordinator | Cluster Controller | Director |
| Process Management | Custom supervision | Erlang supervision trees |
| Error Handling | Fail-fast | "Let it crash" + fail-fast |
| Generation Management | Recovery count | Epoch counter |
| State Persistence | Coordinators | Self-bootstrapping storage |

## Future Enhancements

### Deterministic Simulation
- Implement FoundationDB-style deterministic testing
- Simulate various failure scenarios
- Test recovery under extreme conditions

### Advanced Monitoring
- Component health metrics
- Recovery time tracking
- Failure pattern analysis

### Optimization
- Parallel recovery phases
- Incremental state recovery
- Faster service discovery

This recovery system provides the reliability of FoundationDB with the simplicity and robustness of Erlang/OTP, creating a system that is both fast-recovering and easy to debug.
