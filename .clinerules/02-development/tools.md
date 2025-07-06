## Tools

- **coverage_summary.exs** - Analyzes coverage data for specific modules
  - To verify the test coverage for a module:
    ```bash
    mix coveralls.json && ./scripts/coverage_summary.exs apps/collx/cover/excoveralls.json lib/path/to/module.ex

## Coverage Improvement Process

Follow this systematic 5-step approach for consistent coverage improvements:

1. **Observe**: `mix coveralls.json` to produce an overview report of coverage
   by module.
2. **Analyze**: `./scripts/coverage_summary.exs [coverage-file] [module-path]` 
3. **Write tests** for uncovered functions, focusing on pattern matching branches and edge cases
4. **Verify**: Run tests and check coverage improvement
