# Recovery Guide

**Comprehensive recovery patterns and troubleshooting for Bedrock's distributed system.**

## See Also
- **[Recovery Internals](../01-architecture/recovery-internals.md)** - Complete recovery philosophy and implementation details
- **[Control Plane Components](../03-implementation/control-plane-components.md)** - Director and Coordinator implementation
- **[Debugging Strategies](../02-development/debugging-strategies.md)** - General debugging approaches
- **[Best Practices](../02-development/best-practices.md)** - Error handling patterns

## Recovery Philosophy

Bedrock combines two proven approaches to create a robust, fast-recovering system:

### "Let It Crash" (Erlang/OTP)
- **Fast Failure Detection**: Use `Process.monitor/1` rather than complex health checking
- **Immediate Failure Response**: Any critical component failure triggers immediate director shutdown
- **Supervision Tree Restart**: Let Erlang's supervision trees handle automatic restart
- **Fail-Fast Error Handling**: Prefer immediate failure over complex error recovery

### Fast Recovery Over Complex Error Handling (FoundationDB)
- **Component Failure Triggers Full Recovery**: Any transaction system component failure causes complete recovery
- **Process Suicide**: Processes terminate themselves when they detect newer generations
- **Recovery Count Mechanism**: Each recovery increments an epoch counter for generation management
- **Simple Failure Detection**: Use heartbeats and process monitoring, not complex availability checking

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

## Error Handling Patterns

### Fail-Fast Implementation
```elixir
# GOOD: Fail immediately on error
def get_available_commit_proxy([]), do: exit(:no_commit_proxies)
def get_available_commit_proxy([proxy | _]), do: {:ok, proxy}

# Handle component failure
def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
  Logger.error("Transaction component failed: #{inspect(reason)}")
  # Exit immediately - let coordinator restart us
  exit({:component_failure, reason})
end

# BAD: Complex error handling
def get_available_commit_proxy(proxies) do
  # Complex availability checking, retry logic, etc.
end
```

### Epoch-Based Generation Management
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

### Key Implementation Points
- **Director monitors ALL transaction components**
- **ANY component failure → Director immediate exit**
- **Coordinator uses simple exponential backoff**
- **No circuit breaker complexity**
- **Epoch-based generation management**

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

# Monitor recovery phases
:telemetry.attach_many(
  "recovery-monitor",
  [
    [:bedrock, :director, :recovery, :started],
    [:bedrock, :director, :recovery, :phase_completed],
    [:bedrock, :director, :recovery, :completed]
  ],
  fn event, measurements, metadata, config ->
    IO.inspect({event, measurements, metadata}, label: "RECOVERY")
  end,
  nil
)
```

### Recovery Scenario Debugging

#### Scenario: Recovery Process Hangs
**Symptoms**: System starts but never becomes operational

**Debug Steps**:
1. Check Director logs for which recovery step is failing
2. Verify service discovery is working
3. Check if required services are available
4. Look for deadlocks or infinite loops

```elixir
# Check what recovery phase is active
GenServer.call(:bedrock_director, :get_state)

# Check foreman health
GenServer.call(:bedrock_foreman, :get_workers)

# Check available services
GenServer.call(:bedrock_director, :get_available_services)
```

#### Scenario: System Transaction Fails
**Symptoms**: Recovery completes but system transaction fails

**Debug Steps**:
1. Check sequencer health
2. Verify commit proxy availability
3. Check resolver status
4. Verify log server connectivity

```elixir
# Check transaction components
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "sequencer"))
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "commit_proxy"))
Process.registered() |> Enum.filter(&String.contains?(to_string(&1), "resolver"))
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

## Persistent Configuration and Recovery

### Bootstrap from Storage
The Coordinator supports bootstrapping from persistent storage:
- Reads system configuration from local storage workers on startup
- Uses foreman to discover available storage workers
- Falls back to default configuration if no storage available
- Initializes Raft with storage-derived version (not 0)

### System State Persistence
The Director persists cluster state after successful recovery:
- System transaction tests entire data plane pipeline
- Direct submission to commit proxy (bypassing gateway)
- Fail-fast behavior: transaction failure triggers director restart
- Uses system keyspace (`\xff/system/*`) for cluster state

### Error Recovery Patterns
- **Graceful fallback when storage unavailable**
- **BERT deserialization error recovery**
- **Timeout handling for foreman queries**
- **Corrupted data detection and recovery**

## Performance Characteristics

### Recovery Time
- **Cold Start**: 5-15 seconds (depending on cluster size)
- **Warm Restart**: 1-5 seconds (with persistent configuration)
- **Component Failure**: Sub-second detection, 1-3 second restart

### Scalability
- **Node Count**: Recovery time increases logarithmically with cluster size
- **Data Size**: Storage recovery time depends on transaction log size
- **Component Count**: Linear increase in monitoring overhead

## Testing Recovery

### Unit Testing Recovery Phases
```elixir
# Test recovery phase
test "determining durable version finds highest committed version" do
  # Setup mock log servers with different versions
  # Call the recovery phase
  # Assert correct version is selected
end
```

### Integration Testing
- Test full recovery process with multiple nodes
- Simulate node failures during recovery
- Test configuration changes across the cluster

### Property-Based Testing
- Test recovery under various failure scenarios
- Verify epoch management correctness
- Test configuration consistency

## Best Practices

### Implementation Guidelines
1. **Always use `Process.monitor/1` for component monitoring**
2. **Exit immediately on component failure - don't attempt recovery**
3. **Use epoch counters for generation management**
4. **Implement fail-fast error handling**
5. **Test recovery paths extensively**

### Common Pitfalls to Avoid
1. **Complex error recovery logic**
2. **Partial recovery attempts**
3. **Missing component monitoring**
4. **Inconsistent epoch management**
5. **Inadequate testing of failure scenarios**

### Monitoring and Observability
1. **Use telemetry for recovery progress tracking**
2. **Monitor epoch numbers across components**
3. **Track recovery time and success rates**
4. **Alert on recovery failures or infinite loops**

This recovery guide provides the essential patterns and troubleshooting steps for Bedrock's recovery system, combining the robustness of Erlang/OTP with the proven recovery approach of FoundationDB.