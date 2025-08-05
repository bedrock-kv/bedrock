# Proxy Startup

**Deploys commit proxies for horizontal transaction scalability.**

The Proxy Startup phase deploys multiple [commit proxies](../../components/control-plane/commit-proxy.md) across cluster nodes to enable horizontal transaction processing. Unlike the singleton [sequencer](../../components/control-plane/sequencer.md), proxies can run in parallel to handle increased transaction volume.

## Scalability Through Distribution

Multiple proxies work together to process transactions efficiently:

- **Parallel Processing**: Each proxy handles transaction batches independently
- **Load Distribution**: Incoming transactions spread across available proxies  
- **Independent Operations**: Proxies coordinate with [resolvers](../../components/control-plane/resolver.md) and [log servers](../../components/data-plane/log.md) separately

## Fault-Tolerant Deployment

Proxies deploy using round-robin assignment across coordination-capable nodes to ensure:

- **Physical Isolation**: Hardware failures can't eliminate all transaction processing
- **Resource Distribution**: Prevents resource contention on individual nodes
- **Network Resilience**: Distributed proxies maintain availability during network issues

## Controlled Startup

Deployed proxies remain locked until the [Transaction System Layout](transaction-system-layout.md) phase activates them. This prevents:

- **Premature Processing**: Transactions can't start before the complete pipeline is ready
- **Incomplete Coordination**: Proxies wait for full system layout information
- **Recovery Conflicts**: Locked proxies can't interfere with ongoing recovery

## Critical Requirements

At least one proxy must start successfully or the cluster cannot accept transactions. The phase monitors coordination node availability and ensures minimum viable capacity before proceeding.

---

**Implementation**: [`lib/bedrock/control_plane/director/recovery/proxy_startup_phase.ex`](../../../lib/bedrock/control_plane/director/recovery/proxy_startup_phase.ex)

**Next Phase**: [Resolver Startup](resolver-startup.md) - Deploy MVCC conflict detection components