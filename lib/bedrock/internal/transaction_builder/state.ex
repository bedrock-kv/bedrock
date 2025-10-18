defmodule Bedrock.Internal.TransactionBuilder.State do
  @moduledoc false

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.Tx

  @type t :: %__MODULE__{
          state: :valid | :committed | :rolled_back,
          transaction_system_layout: Bedrock.ControlPlane.Config.TransactionSystemLayout.t(),
          layout_index: LayoutIndex.t(),
          #
          read_version: Bedrock.version() | nil,
          commit_version: Bedrock.version() | nil,
          #
          tx: Tx.t(),
          stack: [Tx.t()],
          fastest_storage_servers: %{Bedrock.key_range() => pid()},
          fetch_timeout_in_ms: pos_integer()
        }
  defstruct state: nil,
            transaction_system_layout: nil,
            layout_index: nil,
            #
            read_version: nil,
            commit_version: nil,
            #
            tx: Tx.new(),
            stack: [],
            fastest_storage_servers: %{},
            fetch_timeout_in_ms: 50,
            #
            active_range_queries: %{}
end
