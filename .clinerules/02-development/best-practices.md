# Development Best Practices for Bedrock

This guide captures lessons learned and best practices for developing Bedrock, based on real development sessions and experience.

## See Also
- **Testing Approaches**: [Testing Strategies](testing-strategies.md) and [Testing Patterns](testing-patterns.md) - Comprehensive testing guidance
- **Debugging Support**: [Debugging Strategies](debugging-strategies.md) - Systematic debugging approaches
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) - Understanding the system design
- **Implementation Guides**: [Control Plane Components](../03-implementation/control-plane-components.md) and [Data Plane Components](../03-implementation/data-plane-components.md)
- **AI Collaboration**: [AI Assistant Guide](../00-start-here/ai-assistant-guide.md) - Effective AI-assisted development patterns

## Code Style and Organization

### Code as Source of Truth Principle

**Rule**: When documentation and implementation disagree, the code is the source of truth.

**Application**: Always verify actual implementation before making assumptions based on documentation. For example, when implementing persistent configuration, the actual `CommitProxy.commit/2` function signature (tuple format) took precedence over documentation examples (map format).

**Benefits**: Prevents implementation bugs, ensures consistency, reduces debugging time.

### DRY (Don't Repeat Yourself) Implementation

**Rule**: Eliminate repetitive code patterns through generic helper functions.

**Pattern**: When you see similar code repeated 3+ times, extract it into a reusable function.

**Example**: Instead of separate `encode_director_reference/2`, `encode_sequencer_reference/2`, etc., create `encode_single_reference/3` that handles all single PID references generically.

**Benefits**: Reduces maintenance burden, improves consistency, makes changes easier to implement across the codebase.

### Leverage Existing Infrastructure

**Rule**: Before creating new mechanisms, thoroughly investigate existing patterns and infrastructure.

**Application**: Use established `otp_name` patterns for process references rather than inventing new naming schemes. Build upon existing behaviours and contracts.

**Benefits**: Consistency with existing patterns, automatic compatibility with new types, reduced maintenance burden.

### Separate Implementation from GenServer Concerns

When building GenServer-based components, separate complex business logic from process lifecycle management by creating dedicated implementation modules.

**Structure Pattern:**
- Main module provides public API and delegates to implementation
- Implementation module (`impl.ex`) contains pure business logic functions
- GenServer module handles only process lifecycle, state management, and message routing

**Benefits:**
- Business logic can be unit tested without GenServer overhead
- Complex algorithms become easier to reason about and debug
- Implementation details are cleanly separated from process concerns
- Enables comprehensive testing of edge cases and error conditions

### Use Long-Form Aliases

Prefer explicit, long-form aliases over grouped imports for better readability and maintainability. Write each alias on its own line to make dependencies more visible and improve code navigation.

### Follow Elixir Style Guide

Avoid `is_` prefixes for predicate functions. Use question marks for boolean-returning functions. Let the formatter handle spacing and indentation consistently.

## Testing Strategies

**Reference**: See [Testing Patterns](testing-patterns.md) for specific testing techniques and [Testing Strategies](testing-strategies.md) for overall testing philosophy.

### Key Testing Principles
- Testing is non-negotiable - always implement comprehensive tests
- Use reliable synchronization patterns (`assert_receive` vs delays)
- Apply DRY principle to test helpers and setup code
- Test edge cases thoroughly (nil values, empty collections, error conditions)
- Structure tests at multiple levels (unit, integration, system)

## Development Workflow Efficiency

### Batch Simple Changes

When making multiple related changes (like alias replacements), combine them
into a single operation rather than making each change separately. This approach
reduces API costs for AI-assisted development, requires fewer compilation
cycles, creates atomic changes that are easier to review, and reduces context
switching between different types of modifications.

### Account for Auto-Formatting

Remember that format-on-save can shift code, affecting diffs. Always use the
final formatted content as reference for subsequent changes, run the formatter
before making diffs, and be aware that spacing, quotes, and line breaks may
change automatically. This prevents confusion when creating search and replace
operations.

### Compile Early and Often

Run `mix compile` after each significant change to catch syntax errors
immediately. This workflow provides several benefits: catching syntax errors
early, verifying dependencies are correct, ensuring changes don't
 break compilation, and providing a faster feedback loop during development.

### Test Incrementally

Follow a systematic testing approach: run unit tests first for the specific
module being changed, then integration tests for interactions with other
components, and finally the full test suite to ensure no regressions across the
system. This layered approach catches issues at the appropriate level of
granularity.

## Architecture Decision Making

### Present Multiple Options

When there are multiple valid approaches, present them with pros and cons to
enable informed decision-making. Consider factors like performance requirements,
extensibility needs, consistency with existing patterns, and maintenance burden.
This collaborative approach helps ensure the best solution is chosen for the
specific context.

### Choose Extensible Solutions

Prefer solutions that automatically support new types without code changes.
Extensible solutions are future-proof against new requirements, reduce
maintenance burden, follow the open/closed principle, and enable plugin-style
architectures. For example, using behaviour callbacks rather than hardcoded
module matching allows new worker types to work automatically.

## Error Handling Patterns

### Graceful Degradation

Handle missing or malformed data gracefully rather than crashing. Design
functions to return sensible defaults when encountering unexpected input, and
structure collection operations to safely filter out invalid items. This
approach makes the system more robust and easier to debug.

### Fail-Fast When Appropriate

Some errors should cause immediate failure rather than silent degradation.
Programming errors, invalid configuration, and contract violations should fail
fast with clear error messages. This prevents subtle bugs from propagating
through the system and makes issues easier to diagnose.

## Performance Considerations

### Avoid Unnecessary Network Calls

When possible, use local data rather than making network requests. Local
manifest data and cached information can often provide the same information
without the overhead and latency of network calls. Reserve network operations
for when they're truly necessary.

### Batch Operations When Possible

Group related operations to reduce overhead. Single-pass operations over
collections are more efficient than multiple passes. Consider the data flow
through your functions and structure them to minimize redundant work.

## Documentation and Knowledge Sharing

### Document Decisions and Rationale

When making architectural decisions, document the reasoning behind the choice.
Include alternative approaches that were considered and why they were rejected.
This context helps future developers understand the trade-offs and makes it
easier to revisit decisions when requirements change.

### Update Knowledge Base

After completing significant work, update the relevant knowledge base sections.
Add new patterns to best practices, document common issues and solutions, update
implementation guides with lessons learned, and add debugging tips for new
components. The knowledge base should be a living resource that grows with the
project.

## Common Pitfalls to Avoid

### Don't Optimize Prematurely

Focus on correctness and clarity first. Write code that is easy to understand
and maintain, then optimize only when performance becomes a measurable problem.
Premature optimization often leads to complex code that is harder to debug and
maintain.

### Don't Ignore Edge Cases

Always consider what happens with unexpected input. Design functions to handle
nil values, empty collections, and malformed data gracefully. Edge case handling
often reveals important assumptions in the code that may not hold in production
environments.

### Don't Hardcode Assumptions

Make code flexible and configurable rather than embedding assumptions about
specific types or values. Use behaviour contracts, configuration parameters, and
pattern matching to create code that adapts to different scenarios without
modification.

This guide should be updated regularly as new patterns and lessons emerge from
development work.
