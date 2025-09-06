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
  value = Repo.get(repo, key)
  Repo.put(repo, key, new_value)
end)
```
