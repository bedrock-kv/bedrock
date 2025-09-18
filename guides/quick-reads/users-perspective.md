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
Repo.transact(fn ->
  value = Repo.get(key)
  Repo.put(key, new_value)
  {:ok, :ok}
end)
```
