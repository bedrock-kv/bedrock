# Bedrock Development Knowledgebase

This knowledgebase contains documentation, guides, and best practices for developing Bedrock, an embedded, distributed key-value store inspired by FoundationDB.

## Directory Structure

- **00-start-here/**: Essential guides for getting started or returning to development
- **01-architecture/**: Architectural concepts, component relationships, and design patterns
- **02-development/**: Development workflows, debugging strategies, and practical guides
- **03-implementation/**: Detailed implementation guides for specific components
- **/docs/plans/**: Detailed plans for making specific, evolutionary changes the system

## Key Documents

### Getting Started

- [Project Reentry Guide](00-start-here/project-reentry-guide.md) - Start here when returning to development
- [Development Setup](00-start-here/development-setup.md) - Environment setup and multi-node testing
- [Quick Reference](00-start-here/quick-reference.md) - Common commands and troubleshooting guide

### Architecture

- [FoundationDB Concepts](01-architecture/foundationdb-concepts.md) - Core architectural concepts
- [Transaction Lifecycle](01-architecture/transaction-lifecycle.md) - End-to-end transaction flow

### Development

- [Best Practices](02-development/best-practices.md) - Development best practices and lessons learned
- [Debugging Strategies](02-development/debugging-strategies.md) - Approaches for debugging distributed systems
- [Testing Strategies](02-development/testing-strategies.md) - Testing philosophy and deterministic simulation ideas
- [Testing Patterns](02-development/testing-patterns.md) - Specific testing techniques and patterns discovered during development

### Implementation

- [Control Plane Components](03-implementation/control-plane-components.md) - Coordinator, Director, and configuration management
- [Data Plane Components](03-implementation/data-plane-components.md) - Sequencer, Commit Proxy, Resolver, Log, and Storage

## Comprehensive Documentation

For a complete architectural overview, see the [Bedrock Architecture Livebook](../docs/bedrock-architecture.livemd).

## Contributing to the Knowledgebase

As you develop Bedrock, consider adding to this knowledgebase:

1. **Implementation Guides**: Add detailed guides for specific components
2. **Common Issues**: Document solutions to problems you encounter
3. **Testing Strategies**: Add patterns for testing distributed behavior
4. **Performance Optimization**: Document performance considerations

The goal is to make this knowledgebase a living resource that grows with the project.
