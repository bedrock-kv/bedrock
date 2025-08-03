# Bedrock Development Knowledgebase

This knowledgebase contains documentation, guides, and best practices for developing Bedrock, an embedded, distributed key-value store inspired by FoundationDB.

## Progressive Loading Structure

This knowledge base is optimized for **token-efficient progressive loading** with three tiers:

### 🔹 Tier 1: Essential Context (<500 tokens each)
**Ultra-fast context loading for immediate development needs:**

- **[AI Context Quick](docs/knowlege_base/00-quick/ai-context-quick.md)** - Core concepts in ~500 tokens
- **[Transaction Quick](docs/knowlege_base/00-quick/transaction-quick.md)** - Essential transaction flow
- **[Components Quick](docs/knowlege_base/00-quick/components-quick.md)** - Component responsibilities
- **[Debug Quick](docs/knowlege_base/00-quick/debug-quick.md)** - Common debugging commands
- **[Testing Quick](docs/knowlege_base/00-quick/testing-quick.md)** - Testing patterns and commands

### 🔹 Tier 2: Task-Specific Guides (<2000 tokens each)
**Focused workflow guides for specific development tasks:**

- **[Recovery Guide](docs/knowlege_base/01-guides/recovery-guide.md)** - Recovery processes and troubleshooting
- **[Architecture Guide](docs/knowlege_base/01-guides/architecture-guide.md)** - Core architectural concepts
- **[Implementation Guide](docs/knowlege_base/01-guides/implementation-guide.md)** - Component implementation patterns
- **[Testing Guide](docs/knowlege_base/01-guides/testing-guide.md)** - Comprehensive testing strategies

### 🔹 Tier 3: Deep References (3000+ tokens)
**Comprehensive documentation for deep implementation work:**

- **[Architecture Deep](docs/knowlege_base/02-deep/architecture-deep.md)** - Complete architectural reference
- **[Development Deep](docs/knowlege_base/02-deep/development-deep.md)** - Comprehensive development practices

## Non-Negotiable Rules
- Always prefer aliases over fully qualified names.
- Always put aliases at the top of modules.
- Always check that a module's (if present) is internally-consistent with the
  code in the module.
- Always check assumptions before presenting them as facts.
- Always check my suggestions and feedback before affirming them.
- Always skim the knowledge base when searching for answers.
- Always use 80-column widths for documentation.
- Never use conditional logic in a test assertions.

## 🚀 Common Workflows

### Getting Started / Returning to Development
1. **[Project Reentry Guide](docs/knowlege_base/00-getting-started/project-reentry-guide.md)** - Start here when returning to development
2. **[Development Setup](docs/knowlege_base/00-getting-started/development-setup.md)** - Environment setup and multi-node testing  
3. **[Quick Reference](docs/knowlege_base/00-getting-started/quick-reference.md)** - Common commands and troubleshooting

### For AI Assistants (Optimized Context Loading)
**Start with minimal context, then load specific sections as needed:**

1. **Essential Context**: [AI Context Quick](docs/knowlege_base/00-quick/ai-context-quick.md)
2. **Task-Specific Quick References**:
   - Recovery work: [Recovery Guide](docs/knowlege_base/01-guides/recovery-guide.md)
   - Transaction work: [Transaction Quick](docs/knowlege_base/00-quick/transaction-quick.md)
   - Component work: [Components Quick](docs/knowlege_base/00-quick/components-quick.md)
   - Testing work: [Testing Quick](docs/knowlege_base/00-quick/testing-quick.md)
   - Debugging: [Debug Quick](docs/knowlege_base/00-quick/debug-quick.md)
3. **Load detailed docs only when needed** for specific implementation work

### By Development Activity

#### 🔍 Debugging Issues
1. **[Debug Quick](docs/knowlege_base/00-quick/debug-quick.md)** - Common commands and patterns
2. **[Recovery Guide](docs/knowlege_base/01-guides/recovery-guide.md)** - Recovery-specific debugging
3. **[Development Deep](docs/knowlege_base/02-deep/development-deep.md)** - Comprehensive debugging strategies

#### 🏗️ Implementing Features
1. **[Components Quick](docs/knowlege_base/00-quick/components-quick.md)** - Component overview
2. **[Implementation Guide](docs/knowlege_base/01-guides/implementation-guide.md)** - Implementation patterns
3. **[Architecture Deep](docs/knowlege_base/02-deep/architecture-deep.md)** - Complete architectural reference

#### 🧪 Writing Tests
1. **[Testing Quick](docs/knowlege_base/00-quick/testing-quick.md)** - Testing commands and patterns
2. **[Testing Guide](docs/knowlege_base/01-guides/testing-guide.md)** - Comprehensive testing strategies
3. **[Development Deep](docs/knowlege_base/02-deep/development-deep.md)** - Advanced testing techniques

#### 🔄 Understanding Transactions
1. **[Transaction Quick](docs/knowlege_base/00-quick/transaction-quick.md)** - Essential transaction flow
2. **[Architecture Guide](docs/knowlege_base/01-guides/architecture-guide.md)** - Transaction lifecycle details
3. **[Architecture Deep](docs/knowlege_base/02-deep/architecture-deep.md)** - Complete transaction processing

## Comprehensive Documentation

For a complete architectural overview with detailed explanations and motivation, see the **[Bedrock Architecture Livebook](docs/bedrock-architecture.livemd)**. This serves as the authoritative reference, while this knowledge base provides practical development guidance.

For detailed transaction processing documentation, see **[Transaction Building and Committing Process](docs/transaction-building-and-committing-process.md)** and the **[Components Deep Dive](docs/components/)** directory.

For system coordination understanding, see the **[Transaction System Layout](docs/transaction-system-layout.md)** specification.

**New to Bedrock?** Start with the **[Glossary](docs/glossary.md)** to understand key terms and concepts.

## Contributing to the Knowledge Base

As you develop Bedrock, consider adding to this knowledge base:

1. **Implementation Guides**: Add detailed guides for specific components
2. **Common Issues**: Document solutions to problems you encounter  
3. **Testing Strategies**: Add patterns for testing distributed behavior
4. **Performance Optimization**: Document performance considerations
5. **Debugging Scenarios**: Add real-world debugging examples and solutions

**📖 See [CONTRIBUTING.md](docs/knowlege_base/CONTRIBUTING.md)** for detailed guidance on maintaining and extending this knowledge base, including document templates, cross-referencing standards, and quality guidelines.
