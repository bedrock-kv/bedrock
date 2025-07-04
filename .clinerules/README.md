# Bedrock Development Knowledgebase

This knowledgebase contains documentation, guides, and best practices for developing Bedrock, an embedded, distributed key-value store inspired by FoundationDB.

## How to Use This Knowledge Base

This knowledge base is organized for both **human developers** and **AI assistants** working on Bedrock. Choose your entry point based on your current needs:

### 🚀 Common Workflows

#### Getting Started / Returning to Development
1. **[Project Reentry Guide](00-start-here/project-reentry-guide.md)** - Start here when returning to development
2. **[Development Setup](00-start-here/development-setup.md)** - Environment setup and multi-node testing  
3. **[Quick Reference](00-start-here/quick-reference.md)** - Common commands and troubleshooting

#### Debugging Issues
1. **[Debugging Strategies](02-development/debugging-strategies.md)** - Systematic debugging approaches
2. **[Component Implementation Guides](03-implementation/)** - Component-specific debugging
3. **[Quick Reference](00-start-here/quick-reference.md)** - Common commands and solutions

#### Testing and Development
1. **[Testing Strategies](02-development/testing-strategies.md)** - Overall testing philosophy and approach
2. **[Testing Patterns](02-development/testing-patterns.md)** - Specific techniques and examples
3. **[Best Practices](02-development/best-practices.md)** - Development guidelines and lessons learned

#### AI Assistant Collaboration
1. **[AI Assistant Guide](00-start-here/ai-assistant-guide.md)** - Structured context and collaboration patterns
2. **[Best Practices](02-development/best-practices.md)** - Development principles and patterns
3. **[Testing Patterns](02-development/testing-patterns.md)** - Testing techniques for AI-assisted development

### 📚 By Topic

#### Recovery and System Startup
- **[Recovery Architecture](01-architecture/foundationdb-concepts.md#system-recovery)** - High-level recovery concepts
- **[Director Recovery Implementation](03-implementation/control-plane-components.md#recovery-process-implementation)** - Detailed recovery phases
- **[Persistent Configuration](01-architecture/persistent-configuration.md)** - Self-bootstrapping cluster state
- **[Debugging Recovery Issues](02-development/debugging-strategies.md#director-recovery-issues)** - Recovery troubleshooting

#### Transaction Processing
- **[Transaction Lifecycle](01-architecture/transaction-lifecycle.md)** - End-to-end transaction flow
- **[Data Plane Components](03-implementation/data-plane-components.md)** - Sequencer, Commit Proxy, Resolver, Log, Storage
- **[MVCC and Conflict Resolution](01-architecture/foundationdb-concepts.md#multi-version-concurrency-control-mvcc)** - Conflict detection concepts

#### Testing and Quality Assurance
- **[Testing Philosophy](02-development/testing-strategies.md)** - Multi-layered testing approach
- **[Testing Patterns](02-development/testing-patterns.md)** - Specific techniques and examples
- **[Component Testing](03-implementation/control-plane-components.md#testing-control-plane-components)** - Testing distributed components
- **[Deterministic Simulation](02-development/testing-strategies.md#deterministic-simulation-testing)** - FoundationDB-style testing ideas

#### Configuration and Persistence
- **[Persistent Configuration Architecture](01-architecture/persistent-configuration.md)** - Self-bootstrapping design
- **[Configuration Management](03-implementation/control-plane-components.md#configuration-management)** - Implementation details
- **[System Transaction Design](01-architecture/persistent-configuration.md#system-transaction-as-comprehensive-test)** - Dual-purpose persistence


## Comprehensive Documentation

For a complete architectural overview with detailed explanations and motivation, see the **[Bedrock Architecture Livebook](../docs/bedrock-architecture.livemd)**. This serves as the authoritative reference, while this knowledge base provides practical development guidance.

## Current Implementation Status

### ✅ Completed Features
- **Persistent Configuration**: Self-bootstrapping cluster state using system's own storage
- **Recovery Process**: Multi-phase director-managed recovery with comprehensive testing
- **Component Structure**: All major control plane and data plane components defined
- **Testing Infrastructure**: Unit, integration, and property-based testing patterns

### 🚧 In Development
- **Complete Transaction Flow**: End-to-end transaction processing integration
- **Multi-Node Coordination**: Distributed system coordination and failure handling
- **Performance Optimization**: Throughput and latency improvements

### 📋 Planned Features
- **Deterministic Simulation**: FoundationDB-style comprehensive testing
- **Automatic Sharding**: Dynamic key range management and rebalancing
- **Advanced Monitoring**: Comprehensive observability and performance tracking

## Contributing to the Knowledge Base

As you develop Bedrock, consider adding to this knowledge base:

1. **Implementation Guides**: Add detailed guides for specific components
2. **Common Issues**: Document solutions to problems you encounter  
3. **Testing Strategies**: Add patterns for testing distributed behavior
4. **Performance Optimization**: Document performance considerations
5. **Debugging Scenarios**: Add real-world debugging examples and solutions

**📖 See [CONTRIBUTING.md](CONTRIBUTING.md)** for detailed guidance on maintaining and extending this knowledge base, including document templates, cross-referencing standards, and quality guidelines.

The goal is to make this knowledge base a living resource that grows with the project and serves both human developers and AI assistants effectively.

## Quick Navigation

- **Need to get started?** → [Project Reentry Guide](00-start-here/project-reentry-guide.md)
- **Debugging an issue?** → [Debugging Strategies](02-development/debugging-strategies.md)
- **Understanding architecture?** → [FoundationDB Concepts](01-architecture/foundationdb-concepts.md)
- **Implementing a feature?** → [Implementation Guides](03-implementation/)
- **Writing tests?** → [Testing Patterns](02-development/testing-patterns.md)
- **AI assistant starting work?** → [AI Assistant Guide](00-start-here/ai-assistant-guide.md)
