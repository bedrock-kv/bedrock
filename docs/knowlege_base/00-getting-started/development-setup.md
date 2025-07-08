# Development Setup Guide

This guide covers the practical setup for developing Bedrock, including working with the multi-node test harness.

## See Also
- **Getting Started**: [Project Reentry Guide](project-reentry-guide.md) - Overview and context for returning to development
- **Quick Commands**: [Quick Reference](quick-reference.md) - Common commands and troubleshooting
- **Debugging Support**: [Debugging Strategies](../02-development/debugging-strategies.md) - Multi-node debugging approaches
- **Architecture Context**: [FoundationDB Concepts](../01-architecture/foundationdb-concepts.md) - Understanding the system design
- **AI Collaboration**: [AI Assistant Guide](ai-assistant-guide.md) - Effective AI-assisted development

## Prerequisites

- Elixir 1.15+ (check with `elixir --version`)
- Erlang/OTP (comes with Elixir)
- Git (for dependency management)

## Initial Setup

### 1. Clone and Setup Dependencies

```bash
# In the bedrock directory
mix deps.get
mix compile
```

### 2. Verify Basic Functionality

```bash
# Run the test suite
mix test

# Check for compilation warnings
mix compile --warnings-as-errors
```

### 3. Setup the Test Harness

The `bedrock_ex` project in the parent directory is your multi-node testing environment:

```bash
# Navigate to the test harness
cd ../bedrock_ex

# Install dependencies
mix deps.get
mix compile
```

## Multi-Node Development Workflow

### Starting a 3-Node Cluster

Open three terminal windows and run these commands:

**Terminal 1 (Node c1):**
```bash
cd /path/to/bedrock_ex
clear; iex --name c1@127.0.0.1 -S mix run
```

**Terminal 2 (Node c2):**
```bash
cd /path/to/bedrock_ex  
clear; iex --name c2@127.0.0.1 -S mix run
```

**Terminal 3 (Node c3):**
```bash
cd /path/to/bedrock_ex
clear; iex --name c3@127.0.0.1 -S mix run
```

### What Should Happen

1. **Node Discovery**: Nodes should discover each other automatically
2. **Raft Election**: One node should be elected as the Raft leader
3. **Recovery Process**: The leader should start the system recovery process
4. **Service Startup**: Various services (logs, storage, etc.) should start

### Checking Cluster Status

In any IEx session, you can check:

```elixir
# See connected nodes
Node.list()

# Check if this should return [:c1@127.0.0.1, :c2@127.0.0.1] (minus current node)

# Try basic cluster operations (these may not work yet)
# This is where you'll identify what needs to be implemented
```

## Development Iteration Cycle

### Making Changes

1. **Edit code** in the `bedrock` project
2. **Recompile** the bedrock project:
   ```bash
   cd /path/to/bedrock
   mix compile
   ```
3. **Restart nodes** in bedrock_ex to pick up changes
4. **Test the changes** in the multi-node environment

### Hot Code Reloading (Advanced)

For faster iteration, you can sometimes reload modules without restarting:

```elixir
# In an IEx session, after recompiling bedrock
r(Bedrock.SomeModule)
```

**Note**: This doesn't always work for GenServers or complex state changes.

## Debugging Multi-Node Issues

### Network Connectivity

```elixir
# Check if nodes can ping each other
Node.ping(:c1@127.0.0.1)  # Should return :pong
Node.ping(:c2@127.0.0.1)  # Should return :pong
```

### Process Inspection

```elixir
# See all processes
Process.list() |> length()

# Start the observer (graphical process viewer)
:observer.start()

# Check supervision trees
Supervisor.which_children(YourSupervisor)
```

### Logging and Tracing

```elixir
# Enable debug logging (if implemented)
Logger.configure(level: :debug)

# Check for telemetry events (if implemented)
# This will depend on your telemetry setup
```

## Common Issues and Solutions

### Nodes Don't Connect
- Check that all nodes use the same cookie (should be automatic)
- Verify the node names are correct
- Check firewall settings (usually not an issue on localhost)

### Compilation Errors
- Run `mix deps.get` to ensure all dependencies are available
- Check that `bedrock_raft` dependency is available at `../bedrock_raft`
- Clear build artifacts: `mix clean && mix compile`

### Raft Election Fails
- Ensure at least 3 nodes are running (quorum requirement)
- Check logs for Raft-related errors
- Verify the Raft configuration

### Recovery Process Hangs
- This is likely where development effort is needed
- Check the Director implementation
- Look for errors in the recovery process logs

## Useful IEx Commands

```elixir
# Reload a module after changes
r(ModuleName)

# Get help on a module
h(ModuleName)

# See module documentation
h(ModuleName.function_name)

# Break out of a hanging process
# Ctrl+C twice, or Ctrl+G then 'q'

# Clear the screen
clear()

# Exit IEx
System.halt()
```

## File Watching (Optional)

For automatic recompilation, you can use:

```bash
# In the bedrock directory
mix test.watch
```

Or set up a file watcher to run `mix compile` when files change.

## Next Steps

Once you have the basic setup working:

1. **Identify what's broken**: Use the multi-node setup to see where the system fails
2. **Focus on one component**: Pick the first failing component and fix it
3. **Add tests**: Write tests for the components you fix
4. **Iterate**: Repeat the process for the next component

## Troubleshooting Resources

- **Architecture Overview**: `docs/bedrock-architecture.livemd`
- **Component Deep Dives**: `.clinerules/03-implementation/`
- **Common Issues**: `.clinerules/02-development/debugging-strategies.md`
