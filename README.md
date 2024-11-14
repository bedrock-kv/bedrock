## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `bedrock` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:bedrock, "~> 0.1"}
  ]
end
```

## About

Bedrock is an embedded, distributed key-value store with guarantees beyond ACID.
It features repeatable reads, strict serialization and a simple API. It is
designed to store your data without cluttering up _your_ code with storage
concerns.

\# In your config.exs file

```elixir
config :my_app,
  bedrock_clusters: [Sample.Cluster]

config :my_app, Sample.Cluster,
  path_to_descriptor: "priv/bedrock.cluster",
  capabilities: [:coordination, :log, :storage]
```

\# In your application code

```elixir
defmodule Sample.Cluster do
  use Bedrock.Cluster,
    otp_app: :my_app,
    name: "sample"
end

defmodule Sample.App do
  import Sample.Cluster.Repo

  def move_money(amount, account1, account2) do
    Repo.transaction(fn repo ->
      with :ok <- check_sufficient_account_balance_for_transfer(repo, account1, amount),
           {:ok, new_balance1} <- adjust_balance(repo, account1, -amount),
           {:ok, new_balance2} <- adjust_balance(repo, account2, amount) do
        {:ok, new_balance1, new_balance2}
      end
    end)
  end

  def check_sufficient_account_balance_for_transfer(repo, account, amount) do
    with {:ok, balance} <- fetch_balance(repo, account) do
      if can_withdraw?(amount, balance) do
        :ok
      else
        {:error, "Insufficient funds"}
      end
    end
  end

  def fetch_balance(repo, account) do
    case Repo.fetch(repo, key_for_account_balance(account)) do
      {:ok, balance} -> {:ok, balance}
      _ -> {:error, "Account not found"}
    end
  end

  def adjust_balance(repo, account, amount) do
    with {:ok, balance} <- fetch_balance(repo, account) do
      new_balance = balance + amount
      Repo.put(repo, key_for_account_balance(account), new_balance)
      {:ok, new_balance}
    end
  end

  def can_withdraw?(amount, balance), do: balance >= amount

  def key_for_account_balance(account), do: "account/balance/#{account}"

  def setup_accounts do
    Repo.transaction(fn repo ->
      Repo.put(repo, key_for_account_balance("1"), 100)
      Repo.put(repo, key_for_account_balance("2"), 500)
      :ok
    end)
  end
end
```

