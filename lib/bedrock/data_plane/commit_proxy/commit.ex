defmodule Bedrock.DataPlane.CommitProxy.Commit do
  @type transaction_info :: {
          GenServer.from(),
          Bedrock.version(),
          read :: [Bedrock.key() | Bedrock.key_range()],
          writes :: %{Bedrock.key() => Bedrock.value()}
        }

  @type t :: %__MODULE__{
          started_at: Bedrock.timestamp_in_ms(),
          last_commit_version: Bedrock.version(),
          next_commit_version: Bedrock.version(),
          n_transactions: non_neg_integer(),
          buffer: [transaction_info()]
        }
  defstruct started_at: nil,
            last_commit_version: nil,
            next_commit_version: nil,
            n_transactions: 0,
            buffer: []

  def new(started_at, last_commit_version, next_commit_version) do
    %__MODULE__{
      started_at: started_at,
      last_commit_version: last_commit_version,
      next_commit_version: next_commit_version,
      n_transactions: 0,
      buffer: []
    }
  end

  defmodule Mutations do
    alias Bedrock.DataPlane.CommitProxy.Commit

    @spec add_transaction(
            t :: Commit.t(),
            from :: GenServer.from(),
            read_version :: Bedrock.version(),
            reads :: [Bedrock.key() | Bedrock.key_range()],
            writes :: %{Bedrock.key() => Bedrock.value()}
          ) :: Commit.t()
    def add_transaction(t, from, read_version, reads, writes) do
      t
      |> Map.update!(:buffer, &[{from, read_version, reads, writes} | &1])
      |> Map.update!(:n_transactions, &(&1.n_transactions + 1))
    end
  end
end
