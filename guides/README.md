# Welcome to Bedrock

Bedrock is an embedded, distributed key-value store that goes beyond traditional ACID guarantees. It provides consistent reads, strict serialization, and transactions across the entire key-space with a simple API.

This documentation is organized to help you find exactly what you need, when you need it. Whether you're getting started, implementing features, or diving deep into the architecture, we've structured the information to match how you actually work.

## How This Documentation Works

We organize information into three levels:

**Quick Reads** - Short, focused explanations that get you oriented fast. Perfect for understanding concepts or explaining Bedrock to others.

**Guides** - Step-by-step instructions and practical information for building with Bedrock. These contain everything you need to implement features or integrate components.

**Deep Dives** - Comprehensive technical coverage with full architectural details. Use these when you need to understand the "why" behind the design or troubleshoot complex issues.

## Getting Started

Start here to understand what Bedrock is and how it works:

- **[User's Perspective](quick-reads/users-perspective.md)** - How you'll actually use Bedrock in your applications
- **[Transaction Basics](quick-reads/transactions.md)** - Core concepts of MVCC and how transactions work

## Understanding Bedrock

For comprehensive technical coverage:

- **[Architecture Analysis](deep-dives/architecture.md)** - Complete architectural patterns and design principles
- **[Control Plane Overview](quick-reads/control-plane.md)** - How cluster coordination and consensus work
- **[Data Plane Overview](quick-reads/data-plane.md)** - Transaction processing and data persistence
- **[Storage Implementations](deep-dives/architecture/implementations/README.md)** - Available storage engines and their
- **[Transaction System Layout](quick-reads/transaction-system-layout.md)** - The big picture of how components work together
- **[Transaction Processing](deep-dives/transactions.md)** - Full details of MVCC implementation and commit protocols
- **[Cluster Startup](deep-dives/cluster-startup.md)** - Bootstrap processes and system initialization
- **[Recovery](quick-reads/recovery.md)** - High-level understanding of system resilience ([More Detail](deep-dives/recovery.md))

## Reference

- **[Glossary](glossary.md)** - Complete terminology reference with cross-linked definitions
