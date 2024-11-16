defmodule Bedrock.Cluster.Gateway.TransactionBuilder.State do
  @type t :: %__MODULE__{
          state: :valid | :committed | :rolled_back | :expired,
          gateway: pid(),
          storage_table: :ets.table(),
          key_codec: module(),
          value_codec: module(),
          #
          read_version: Bedrock.version() | nil,
          read_version_lease_expiration: integer() | nil,
          #
          reads: %{},
          writes: %{},
          stack: [%{reads: map(), writes: map()}],
          fastest_storage_servers: %{Bedrock.key_range() => pid()},
          fetch_timeout_in_ms: pos_integer(),
          lease_renewal_threshold: pos_integer()
        }
  defstruct state: nil,
            gateway: nil,
            storage_table: nil,
            key_codec: nil,
            value_codec: nil,
            #
            read_version: nil,
            read_version_lease_expiration: nil,
            #
            reads: %{},
            writes: %{},
            stack: [],
            fastest_storage_servers: %{},
            fetch_timeout_in_ms: 50,
            lease_renewal_threshold: 100
end
