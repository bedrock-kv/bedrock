# CoordinatorClient Component Deep Dive

The [CoordinatorClient](../../../glossary.md#gateway) is the client-facing interface component that manages [transaction](../../../glossary.md#transaction) coordination and serves as the entry point for all client operations in the Bedrock system.

## Overview

The CoordinatorClient acts as the intermediary between client applications and the distributed data plane components. It abstracts the complexity of the distributed system while providing clients with a simple transaction interface.

**Location**: `lib/bedrock/cluster/gateway.ex`

## Embedded Distributed Architecture

The CoordinatorClient represents a fundamental shift in how distributed databases interface with applications. Rather than acting as a network service that clients connect to remotely, the CoordinatorClient embeds directly within application processes, providing local transaction coordination while participating in global distributed protocols.

### Local-First Transaction Processing

The CoordinatorClient implements Bedrock's core embedded distributed principle: processing within application boundaries rather than across network boundaries. When applications initiate transactions, they're not making network calls to remote database servers—instead, they're invoking local CoordinatorClient processes that coordinate distributed operations on their behalf.

This local-first approach transforms transaction semantics. Applications experience sub-microsecond transaction initiation times because CoordinatorClient processes are co-located within the same memory space. Resource coordination happens locally, eliminating the network latency that dominates traditional distributed database operations.

The CoordinatorClient's transaction management also enables new programming patterns. Because transaction coordination is local, applications can initiate hundreds or thousands of concurrent transactions with minimal overhead, supporting fine-grained transactional workflows that would be prohibitively expensive in client-server architectures.

### Unified Failure Domain Coordination

One of the CoordinatorClient's most important architectural contributions is how it leverages embedded distributed systems' simplified failure scenarios. In traditional distributed databases, applications must handle complex failure modes: what happens when the application is healthy but the database is unavailable? Or when network partitions isolate clients from servers?

The CoordinatorClient eliminates these scenarios by creating unified failure domains. When an application fails, its embedded CoordinatorClient fails with it, ensuring there are no orphaned transactions or leaked resources. This unified failure approach dramatically simplifies both application error handling and distributed system recovery protocols.

The CoordinatorClient's [worker advertisement](../../../glossary.md#worker-advertisement) functionality exemplifies this design. Rather than maintaining separate service discovery systems, the CoordinatorClient leverages the fact that it knows directly when local workers start or stop—because they're all part of the same failure domain. This knowledge enables more efficient and reliable cluster coordination than traditional service discovery mechanisms.

### Embedded Distributed Advantages

The CoordinatorClient's embedded architecture enables capabilities impossible in client-server systems. Because CoordinatorClient processes share memory space with applications, they can implement zero-copy transaction builders, eliminating serialization overhead for transaction state management. They can also provide application-aware optimizations, such as transaction batching based on application request patterns.

The embedded approach also transforms operational characteristics. The CoordinatorClient ensures that transaction capabilities are always available when applications start—there's no separate database service to connect to or dependency to manage. Applications and their transaction coordination deploy and scale together as single units, eliminating the operational complexity of coordinating application and database infrastructure.

This design also enables sophisticated performance optimizations. The CoordinatorClient can learn from application transaction patterns, pre-warm [transaction system layouts](../../../glossary.md#transaction-system-layout) for frequently used data ranges, and coordinate with local storage workers to optimize data placement for application access patterns.

## Core Responsibilities

### 1. Transaction Lifecycle Management

- **Transaction Initiation**: Creates new [Transaction Builder](../../../glossary.md#transaction-builder) processes for each transaction
- **Resource Coordination**: Manages the lifecycle of transaction-related resources
- **Client Interface**: Provides the primary API surface for client operations

### 2. Worker Advertisement

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
2. Passes gateway reference and [transaction system layout](../../../glossary.md#transaction-system-layout) to builder
3. Returns the transaction builder PID for subsequent operations

**Usage**:

```elixir
{:ok, transaction_pid} = CoordinatorClient.begin_transaction(gateway)
```

### Worker Advertisement

```elixir
@spec advertise_worker(gateway :: ref(), worker :: pid()) :: :ok
```

**Purpose**: Handles dynamic [worker](../../../glossary.md#worker) registration for cluster membership.

**Process**:

1. Receives worker PID from newly started workers
2. Interrogates worker for capability information
3. Forwards worker details to cluster [director](../../../glossary.md#director)
4. Always returns `:ok` (asynchronous operation)

## Implementation Details

### Server Architecture

The CoordinatorClient uses the `Bedrock.Internal.GenServerApi` pattern:

- **API Module**: `Bedrock.Cluster.CoordinatorClient` - Client-facing functions
- **Server Module**: `Bedrock.Cluster.CoordinatorClient.Server` - GenServer implementation
- **State Module**: `Bedrock.Cluster.CoordinatorClient.State` - State management

### State Management

**Key State Components**:

```elixir
%State{
  cluster: Cluster.t(),
  transaction_system_layout: TransactionSystemLayout.t(),
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
- **[Sequencer](../../../glossary.md#sequencer)**: Coordinates read version assignments through builders

## Performance Characteristics

### Scalability

- **Stateless Operations**: Most operations are stateless for horizontal scaling
- **Worker Advertisement**: Asynchronous processing prevents blocking

### Latency Sources

1. **Transaction Creation**: Process spawning overhead for new transactions
2. **State Lookups**: In-memory state access (minimal impact)

### Optimization Strategies

- **Process Pooling**: Could implement transaction builder pooling
- **State Caching**: Cache frequently accessed transaction system layout

## Error Handling

### Common Error Scenarios

**Transaction Creation Failures**:

```elixir
{:error, :timeout} # System overloaded, cannot create transaction builder
```

**[Recovery](../../../glossary.md#recovery) Behavior**:

- **Process Failures**: CoordinatorClient process restart requires transaction retry
- **Network Partitions**: Transactions may need to be retried
- **Overload**: New transaction creation may be throttled

### Monitoring and Observability

**Key Metrics**:

- Active transaction count
- Worker advertisement rate
- Transaction creation latency

**Telemetry Events**:

- Transaction lifecycle events
- Worker advertisement processing

## Configuration

**Key Configuration Options**:

- **Transaction Timeout**: Maximum transaction lifetime
- **Worker Advertisement Buffer**: Batching for worker advertisements

## Testing Considerations

### Unit Testing

- Mock transaction system layout for isolated testing
- Verify worker advertisement forwarding

### Integration Testing

- Test with real Transaction Builders
- Test worker advertisement flow with Director

### Load Testing

- High-frequency transaction creation
- Worker advertisement bursts

## Future Enhancements

### Potential Improvements

1. **Connection Pooling**: Manage persistent connections to [data plane](../../../glossary.md#data-plane) components
2. **Load Balancing**: Intelligent routing of transactions to less loaded components
3. **Caching**: Cache transaction system layout and worker information

### Monitoring Enhancements

1. **Health Checks**: Periodic validation of downstream component availability
2. **Performance Metrics**: Detailed latency and throughput tracking
3. **Alert Integration**: Proactive alerting on creation failures

## Cross-Component Workflow Integration

### Transaction Processing Flow Role

The CoordinatorClient serves as the **entry point** in Bedrock's core transaction processing workflow:

**CoordinatorClient → Transaction Builder → Commit Proxy → Resolver → Sequencer → Storage**

**Workflow Context**:

1. **Transaction Initiation**: Client requests arrive at CoordinatorClient, which creates dedicated Transaction Builder processes
2. **Resource Management**: CoordinatorClient manages transaction lifecycle and ensures clean resource cleanup
3. **Error Propagation**: Transaction failures propagate back through CoordinatorClient to clients

For the complete transaction flow, see **[Transaction Processing Deep Dive](../../../deep-dives/transactions.md)**.

### Service Registration Workflow Role

The CoordinatorClient participates in the **service discovery and registration** workflow:

**Foreman → CoordinatorClient → Coordinator → Director**

**Workflow Context**:

1. **Worker Advertisement**: CoordinatorClient receives worker advertisements from local processes
2. **Service Discovery**: CoordinatorClient forwards worker information to Coordinator for cluster-wide registration
3. **Directory Integration**: Coordinator maintains authoritative service directory for Director recovery coordination

**Handoff Points**:

- **From Foreman**: Receives worker advertisements via `advertise_worker/2`
- **To Coordinator**: Forwards service information for consensus-based registration
- **Error Handling**: Advertisement failures are asynchronous and don't block client operations

## Related Components

- **[Transaction Builder](transaction-builder.md)**: Process created by CoordinatorClient for each transaction
- **[Sequencer](../data-plane/sequencer.md)**: Coordinates with CoordinatorClient for read version management
- **[Director](../control-plane/director.md)**: Control plane component that receives worker advertisements from CoordinatorClient
- **[Coordinator](../control-plane/coordinator.md)**: Control plane component that provides transaction system layout to CoordinatorClient
- **[Foreman](foreman.md)**: Infrastructure component that advertises workers to CoordinatorClient

## Code References

- **Main API**: `lib/bedrock/cluster/gateway.ex`
- **Server Implementation**: `lib/bedrock/cluster/gateway/server.ex`
- **State Management**: `lib/bedrock/cluster/gateway/state.ex`
- **Worker Advertisement**: `lib/bedrock/cluster/gateway/worker_advertisement.ex`
- **Discovery Logic**: `lib/bedrock/cluster/gateway/discovery.ex`
