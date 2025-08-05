# Gateway Component Deep Dive

The [Gateway](../glossary.md#gateway) is the client-facing interface component that manages [transaction](../glossary.md#transaction) coordination, [read version](../glossary.md#read-version) [leasing](../glossary.md#lease), and serves as the entry point for all client operations in the Bedrock system.

## Overview

The Gateway acts as the intermediary between client applications and the distributed data plane components. It abstracts the complexity of the distributed system while providing clients with a simple transaction interface.

**Location**: `lib/bedrock/cluster/gateway.ex`

## Core Responsibilities

### 1. Transaction Lifecycle Management
- **Transaction Initiation**: Creates new [Transaction Builder](../glossary.md#transaction-builder) processes for each transaction
- **Resource Coordination**: Manages the lifecycle of transaction-related resources
- **Client Interface**: Provides the primary API surface for client operations

### 2. Read Version Leasing
- **Version Leasing**: Manages read version leases with expiration times
- **[Lease Renewal](../glossary.md#lease-renewal)**: Handles automatic lease renewal for long-running transactions
- **Lease Tracking**: Maintains lease state and expiration monitoring

### 3. Worker Advertisement
- **Service Discovery**: Receives advertisements from new workers joining the cluster
- **Director Communication**: Forwards worker information to the cluster director
- **Cluster Coordination**: Facilitates dynamic cluster membership management

## Key APIs

### Transaction Management

```elixir
@spec begin_transaction(gateway_ref :: ref(), opts :: keyword()) :: 
  {:ok, transaction_pid :: pid()} | {:error, :timeout}
```

**Purpose**: Initiates a new transaction by creating a Transaction Builder process.

**Process**:
1. Creates a new Transaction Builder process via `start_link/1`
2. Passes gateway reference and [transaction system layout](../glossary.md#transaction-system-layout) to builder
3. Returns the transaction builder PID for subsequent operations

**Usage**:
```elixir
{:ok, transaction_pid} = Gateway.begin_transaction(gateway)
```

### Read Version Lease Management

```elixir
@spec renew_read_version_lease(gateway_ref :: ref(), read_version :: Bedrock.version(), opts :: keyword()) ::
  {:ok, lease_deadline_ms :: Bedrock.interval_in_ms()} | {:error, :lease_expired}
```

**Purpose**: Renews or establishes a lease for a specific read version.

**Process**:
1. Validates the read version is still valid
2. Calculates new lease expiration time
3. Updates internal lease tracking
4. Returns lease deadline in milliseconds

**Lease Management Strategy**:
- Default lease duration: configurable per transaction
- Automatic renewal when lease approaches expiration
- Lease expiration causes transaction to abort

### Worker Advertisement

```elixir
@spec advertise_worker(gateway :: ref(), worker :: pid()) :: :ok
```

**Purpose**: Handles dynamic [worker](../glossary.md#worker) registration for cluster membership.

**Process**:
1. Receives worker PID from newly started workers
2. Interrogates worker for capability information
3. Forwards worker details to cluster [director](../glossary.md#director)
4. Always returns `:ok` (asynchronous operation)

## Implementation Details

### Server Architecture

The Gateway uses the `Bedrock.Internal.GenServerApi` pattern:
- **API Module**: `Bedrock.Cluster.Gateway` - Client-facing functions
- **Server Module**: `Bedrock.Cluster.Gateway.Server` - GenServer implementation
- **State Module**: `Bedrock.Cluster.Gateway.State` - State management

### State Management

**Key State Components**:
```elixir
%State{
  cluster: Cluster.t(),
  transaction_system_layout: TransactionSystemLayout.t(),
  active_leases: %{read_version => lease_info},
  worker_advertisements: [worker_info],
  # ... other state
}
```

### Integration Points

**Upstream Dependencies**:
- **Coordinator**: Receives transaction system layout updates
- **Director**: Forwards worker advertisements and receives cluster state

**Downstream Dependencies**:
- **Transaction Builder**: Creates and manages transaction processes
- **[Sequencer](../glossary.md#sequencer)**: Coordinates read version assignments through builders

## Performance Characteristics

### Scalability
- **Stateless Operations**: Most operations are stateless for horizontal scaling
- **Lease Management**: Minimal state required for lease tracking
- **Worker Advertisement**: Asynchronous processing prevents blocking

### Latency Sources
1. **Transaction Creation**: Process spawning overhead for new transactions
2. **Lease Renewal**: Network round-trip to validate and renew leases
3. **State Lookups**: In-memory state access (minimal impact)

### Optimization Strategies
- **Process Pooling**: Could implement transaction builder pooling
- **Lease [Batching](../glossary.md#batching)**: [Batch](../glossary.md#batch) lease renewals for multiple transactions
- **State Caching**: Cache frequently accessed transaction system layout

## Error Handling

### Common Error Scenarios

**Transaction Creation Failures**:
```elixir
{:error, :timeout} # System overloaded, cannot create transaction builder
```

**Lease Expiration**:
```elixir
{:error, :lease_expired} # Read version lease has expired
```

**[Recovery](../glossary.md#recovery) Behavior**:
- **Process Failures**: Gateway process restart loses active leases
- **Network Partitions**: Lease renewals fail, causing transaction aborts
- **Overload**: New transaction creation may be throttled

### Monitoring and Observability

**Key Metrics**:
- Active transaction count
- Lease renewal frequency and success rate
- Worker advertisement rate
- Transaction creation latency

**Telemetry Events**:
- Transaction lifecycle events
- Lease management events
- Worker advertisement processing

## Configuration

**Key Configuration Options**:
- **Default Lease Duration**: How long read version leases last
- **Lease Renewal Threshold**: When to trigger automatic renewal
- **Transaction Timeout**: Maximum transaction lifetime
- **Worker Advertisement Buffer**: Batching for worker advertisements

## Testing Considerations

### Unit Testing
- Mock transaction system layout for isolated testing
- Test lease expiration and renewal logic
- Verify worker advertisement forwarding

### Integration Testing
- Test with real Transaction Builders
- Verify lease coordination with Sequencer
- Test worker advertisement flow with Director

### Load Testing
- High-frequency transaction creation
- Concurrent lease renewals
- Worker advertisement bursts

## Future Enhancements

### Potential Improvements
1. **Connection Pooling**: Manage persistent connections to [data plane](../glossary.md#data-plane) components
2. **Lease Optimization**: Predictive lease renewal based on transaction patterns
3. **Load Balancing**: Intelligent routing of transactions to less loaded components
4. **Caching**: Cache transaction system layout and worker information

### Monitoring Enhancements
1. **Health Checks**: Periodic validation of downstream component availability
2. **Performance Metrics**: Detailed latency and throughput tracking
3. **Alert Integration**: Proactive alerting on lease expiration or creation failures

## Related Components

- **[Transaction Builder](../control-plane/transaction-builder.md)**: Process created by Gateway for each transaction
- **[Sequencer](../control-plane/sequencer.md)**: Coordinates with Gateway for read version management
- **Director**: Control plane component that receives worker advertisements from Gateway
- **Coordinator**: Control plane component that provides transaction system layout to Gateway

## Code References

- **Main API**: `lib/bedrock/cluster/gateway.ex`
- **Server Implementation**: `lib/bedrock/cluster/gateway/server.ex`
- **State Management**: `lib/bedrock/cluster/gateway/state.ex`
- **Worker Advertisement**: `lib/bedrock/cluster/gateway/worker_advertisement.ex`
- **Discovery Logic**: `lib/bedrock/cluster/gateway/discovery.ex`