# Commit Proxy Startup

**Deploys commit proxies for horizontal transaction scalability.**

The Commit Proxy Startup phase deploys multiple [commit proxies](../../components/data-plane/commit-proxy.md) across cluster nodes to enable horizontal transaction processing. Unlike the singleton [sequencer](../../components/data-plane/sequencer.md), commit proxies can run in parallel to handle increased transaction volume.

## Scalability Through Distribution

Multiple commit proxies work together to process transactions efficiently:

- **Parallel Processing**: Each commit proxy handles transaction batches independently
- **Load Distribution**: Incoming transactions spread across available commit proxies  
- **Independent Operations**: Commit proxies coordinate with [resolvers](../../components/data-plane/resolver.md) and [log servers](../../components/data-plane/log.md) separately

## Fault-Tolerant Deployment

Commit proxies deploy using round-robin assignment across coordination-capable nodes to ensure:

- **Physical Isolation**: Hardware failures can't eliminate all transaction processing
- **Resource Distribution**: Prevents resource contention on individual nodes
- **Network Resilience**: Distributed commit proxies maintain availability during network issues

## Controlled Startup

Deployed commit proxies remain locked until the [Transaction System Layout](transaction-system-layout.md) phase activates them. This prevents:

- **Premature Processing**: Transactions can't start before the complete pipeline is ready
- **Incomplete Coordination**: Commit proxies wait for full system layout information
- **Recovery Conflicts**: Locked commit proxies can't interfere with ongoing recovery

## Critical Requirements

At least one commit proxy must start successfully or the cluster cannot accept transactions. The phase monitors coordination node availability and ensures minimum viable capacity before proceeding.

---

**Implementation**: [`lib/bedrock/control_plane/director/recovery/proxy_startup_phase.ex`](../../../../lib/bedrock/control_plane/director/recovery/proxy_startup_phase.ex)

**Next Phase**: [Resolver Startup](resolver-startup.md) - Deploy MVCC conflict detection components
