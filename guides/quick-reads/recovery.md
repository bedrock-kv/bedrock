# Bedrock Recovery

**For comprehensive details and deeper context, see the [Recovery Guide](../deep-dives/recovery.md).**

## Recovery Flow

```mermaid
flowchart TD
    Start[Recovery Start] --> TSL[TSL Validation]
    TSL --> Decision{Old TSL Exists?}
    Decision -->|No| Init[Initialization Phase]
    Decision -->|Yes| Locking[Service Locking]
    
    %% Service locking leads to log planning
    Locking --> LogPlan[Log Recovery Planning]
    
    %% New cluster path (shorter)
    Init --> LogRecruit[Log Recruitment]
    
    %% Existing cluster recovery path
    LogPlan --> Vacancy[Vacancy Creation]
    Vacancy --> Version[Version Determination]
    Version --> LogRecruit
    
    %% Convergence point - both paths merge here
    LogRecruit --> Storage[Storage Recruitment]
    Storage --> Replay[Log Replay]
    Replay --> Sequencer[Sequencer Startup]
    Sequencer --> CommitProxy[Commit Proxy Startup]
    CommitProxy --> Resolver[Resolver Startup]
    Resolver --> Layout[Transaction System Layout]
    Layout --> Persist[Persistence]
    Persist --> Monitor[Monitoring]
    Monitor --> Complete[Recovery Complete]
    
    %% Styling
    style Start fill:#e1f5fe
    style TSL fill:#f3e5f5
    style Complete fill:#e8f5e8
    style Decision fill:#fff3e0
    style Init fill:#f3e5f5
    style LogPlan fill:#fce4ec
    style Vacancy fill:#fce4ec
    style Version fill:#fce4ec
    style LogRecruit fill:#e8eaf6
    style Storage fill:#e8eaf6
    style Replay fill:#f1f8e9
    style Sequencer fill:#fff8e1
    style CommitProxy fill:#fff8e1
    style Resolver fill:#fff8e1
    style Layout fill:#e0f2f1
    style Persist fill:#e0f2f1
    style Monitor fill:#e0f2f1
```

## Recovery Phases

Recovery proceeds through a carefully orchestrated sequence of phases, each building upon the previous one:

### Foundation Phases

0. **[TSL Validation](recovery/tsl-validation.md)** - Validate type safety of recovered TSL data structure
1. **[Service Locking](recovery/service-locking.md)** - Establish exclusive control over old system services (includes path determination logic)

### Data Recovery Path

2. **[Log Recovery Planning](recovery/log-recovery-planning.md)** - Determine what transaction data can be safely recovered
3. **[Vacancy Creation](recovery/vacancy-creation.md)** - Plan the new system architecture with placeholders
4. **[Version Determination](recovery/version-determination.md)** - Establish the recovery baseline for durable data

### Service Recruitment

5. **[Log Recruitment](recovery/log-recruitment.md)** - Assign real services to log vacancy placeholders
6. **[Storage Recruitment](recovery/storage-recruitment.md)** - Assign real services to storage vacancy placeholders

### Data Migration

7. **[Log Replay](recovery/log-replay.md)** - Copy committed transactions to new log services

### Component Startup

8. **[Sequencer Startup](recovery/sequencer-startup.md)** - Start the global version number authority
9. **[Commit Proxy Startup](recovery/proxy-startup.md)** - Deploy commit proxy components for horizontal transaction scalability
10. **[Resolver Startup](recovery/resolver-startup.md)** - Start MVCC conflict detection components

### System Finalization

11. **[Transaction System Layout](recovery/transaction-system-layout.md)** - Create the coordination blueprint
12. **[Persistence](recovery/persistence.md)** - Durably store configuration via system transaction
13. **[Monitoring](recovery/monitoring.md)** - Establish operational monitoring and mark recovery complete

## Recovery Entry Point

Recovery begins when the Director creates a `RecoveryAttempt` with the current timestamp, cluster configuration, and epoch. This initialization occurs in `RecoveryAttempt.new/3` and establishes the timing baseline for the entire recovery process. The recovery attempt tracks all state changes as recovery progresses through its phases, starting with TSL validation to ensure type safety before proceeding to service coordination.

## Implementation References

- **Main Recovery Module**: `lib/bedrock/control_plane/director/recovery.ex`
- **Phase Implementations**: `lib/bedrock/control_plane/director/recovery/*_phase.ex`
- **Recovery Attempt State**: `lib/bedrock/control_plane/config/recovery_attempt.ex`

## See Also

- [Recovery Deep Dive](../deep-dives/recovery.md) - Comprehensive recovery system analysis
- [Bedrock Architecture](../deep-dives/architecture.md) - Overall system architecture
- [Components Documentation](../deep-dives/architecture/README.md) - Individual component details
- [Transaction System Layout](transaction-system-layout.md) - System coordination blueprint
