# Contributing to the Bedrock Knowledge Base

This guide explains how to maintain and extend the knowledge base as the project evolves.

## Knowledge Base Principles

### 1. Living Documentation
- Update documents as you learn and discover new patterns
- Add real debugging scenarios and solutions as they occur
- Document architectural decisions and their rationale

### 2. Cross-Reference Everything
- Always add "See Also" sections to new documents
- Update existing cross-references when adding new content
- Maintain bidirectional links between related concepts

### 3. Multiple Entry Points
- Consider different user contexts (debugging, implementing, learning)
- Provide both workflow-based and topic-based navigation
- Include quick references for common scenarios

## Adding New Documents

### Required Sections
Every new document should include:

```markdown
# Document Title

Brief description of what this document covers.

## See Also
- **Related Architecture**: [Link to architectural concepts]
- **Implementation Details**: [Link to implementation guides]
- **Development Support**: [Link to debugging/testing guides]
- **Prerequisites**: [Link to required background knowledge]

## Content...
```

### Update Main README
When adding new documents:
1. Add to appropriate section in main README
2. Consider if it needs a workflow entry point
3. Add to thematic navigation if it introduces new topics
4. Update quick navigation if it's frequently referenced

## Maintaining Cross-References

### When Adding New Content
- Scan existing documents for related concepts
- Add forward references from existing docs to new content
- Update the main README if the new content changes navigation

### When Updating Existing Content
- Check if cross-references need updates
- Verify that linked sections still exist
- Update status information if implementation progress changes

## Document Templates

### Architecture Document Template
```markdown
# [Component/Concept] Architecture

Description of the architectural concept or component.

## See Also
- **Related Concepts**: [Links to related architecture docs]
- **Implementation**: [Links to implementation guides]
- **Development**: [Links to debugging/testing approaches]

## Overview
High-level description and purpose.

## Key Concepts
Core concepts and relationships.

## Implementation Considerations
How this affects development and debugging.
```

### Implementation Guide Template
```markdown
# [Component] Implementation Guide

Detailed implementation guidance for [component].

## See Also
- **Architecture**: [Link to architectural concepts]
- **Related Components**: [Links to related implementation guides]
- **Development Support**: [Links to debugging/testing]

## Overview
What this component does and why.

## Implementation Details
Specific implementation guidance.

## Testing Approaches
How to test this component.

## Common Issues
Known problems and solutions.

## Debugging Tips
Specific debugging approaches for this component.
```

### Development Guide Template
```markdown
# [Topic] Development Guide

Practical guidance for [development topic].

## See Also
- **Architecture Context**: [Links to relevant architecture]
- **Implementation Details**: [Links to specific components]
- **Related Practices**: [Links to related development guides]

## Overview
What this guide covers and when to use it.

## Key Principles
Core principles and approaches.

## Practical Examples
Real-world examples and patterns.

## Common Pitfalls
What to avoid and why.
```

## Extending Navigation

### Adding New Workflows
When you identify a new common workflow:
1. Add it to the main README under "Common Workflows"
2. Create a logical sequence of documents to follow
3. Consider if it needs its own quick reference section

### Adding New Topics
When introducing new thematic areas:
1. Add a new section under "By Topic" in main README
2. Ensure all related documents cross-reference the new topic
3. Consider if it needs representation in quick navigation

## Quality Guidelines

### Writing Style
- Be concise but comprehensive
- Use active voice and clear language
- Include practical examples where helpful
- Focus on actionable guidance

### Cross-Reference Quality
- Make link text descriptive (not just "see here")
- Explain why the cross-reference is relevant
- Group related links logically
- Keep cross-reference lists manageable (5-7 items max)

### Maintenance
- Review cross-references when restructuring documents
- Update status information regularly
- Remove or update outdated information
- Verify that all links work

## Examples of Good Contributions

### Adding a New Debugging Scenario
1. Add the scenario to debugging-strategies.md
2. Cross-reference from relevant component implementation guides
3. Add to quick-reference.md if it's a common issue
4. Update main README if it represents a new category of problem

### Documenting a New Component
1. Create implementation guide using the template
2. Add architecture section if it introduces new concepts
3. Update main README navigation
4. Add cross-references from related existing documents
5. Update AI assistant guide with new patterns if relevant

### Adding New Testing Patterns
1. Add to testing-patterns.md with examples
2. Cross-reference from testing-strategies.md
3. Update component implementation guides with testing approaches
4. Add to AI assistant guide if it affects collaboration patterns

## Review Checklist

Before adding new content, verify:
- [ ] Document follows established template structure
- [ ] Cross-references are comprehensive and bidirectional
- [ ] Main README navigation is updated if needed
- [ ] Content is actionable and practical
- [ ] Examples are included where helpful
- [ ] Links are tested and work correctly

This approach ensures the knowledge base remains a living, useful resource that grows effectively with the project.
