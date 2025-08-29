defmodule Bedrock.Cluster.Gateway.TransactionBuilder.State do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @type t :: %__MODULE__{
          state: :valid | :committed | :rolled_back | :expired,
          gateway: pid(),
          transaction_system_layout: Bedrock.ControlPlane.Config.TransactionSystemLayout.t(),
          layout_index: LayoutIndex.t(),
          key_codec: module(),
          value_codec: module(),
          #
          read_version: Bedrock.version() | nil,
          read_version_lease_expiration: integer() | nil,
          commit_version: Bedrock.version() | nil,
          #
          tx: Tx.t(),
          stack: [Tx.t()],
          fastest_storage_servers: %{Bedrock.key_range() => pid()},
          fetch_timeout_in_ms: pos_integer(),
          lease_renewal_threshold: pos_integer()
        }
  defstruct state: nil,
            gateway: nil,
            transaction_system_layout: nil,
            layout_index: nil,
            key_codec: nil,
            value_codec: nil,
            #
            read_version: nil,
            read_version_lease_expiration: nil,
            commit_version: nil,
            #
            tx: Tx.new(),
            stack: [],
            fastest_storage_servers: %{},
            fetch_timeout_in_ms: 50,
            lease_renewal_threshold: 100,
            #
            active_range_queries: %{}
end
