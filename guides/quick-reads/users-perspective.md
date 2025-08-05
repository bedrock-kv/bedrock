# Defining a Cluster

```elixir
defmodule ExampleApp.Cluster do
  use ExampleApp.Cluster,
    otp_app: :example_app,
    name: "example"
end

```

## Executing a Transaction

```elixir
# Basic transaction
Repo.transaction(fn repo ->
  value = Repo.fetch(repo, key)
  Repo.put(repo, key, new_value)
end)

# Read-only transaction (no commit phase)
Repo.snapshot(fn repo ->
  Repo.fetch(repo, key)
end)
```