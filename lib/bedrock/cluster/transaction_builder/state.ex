defmodule Bedrock.Cluster.TransactionBuilder.State do
  @type t :: %__MODULE__{
          state: :valid | :committed | :rolled_back | :expired,
          gateway: pid(),
          transaction_system_layout: map(),
          #
          read_version: Bedrock.version() | nil,
          read_version_lease_expiration: integer() | nil,
          #
          reads: %{},
          writes: %{},
          stack: [%{reads: map(), writes: map()}],
          storage_servers: map(),
          fetch_timeout_in_ms: pos_integer(),
          lease_renewal_threshold: pos_integer()
        }
  defstruct state: nil,
            gateway: nil,
            transaction_system_layout: nil,
            #
            read_version: nil,
            read_version_lease_expiration: nil,
            #
            reads: %{},
            writes: %{},
            stack: [],
            storage_servers: %{},
            fetch_timeout_in_ms: 50,
            lease_renewal_threshold: 100
end
