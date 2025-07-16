defmodule Bedrock.Cluster.Gateway.State do
  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type t :: %__MODULE__{
          node: node(),
          cluster: module(),
          path_to_descriptor: Path.t(),
          descriptor: Descriptor.t(),
          coordinator: Coordinator.ref() | :unavailable,
          director: {director :: pid(), Bedrock.epoch()} | :unavailable,
          timers: %{atom() => reference()} | nil,
          missed_pongs: non_neg_integer(),
          mode: :passive | :active,
          capabilities: [Bedrock.Cluster.capability()],
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          storage_table: :ets.table(),
          #
          deadline_by_version: %{Bedrock.version() => Bedrock.timestamp_in_ms()},
          minimum_read_version: Bedrock.version() | nil,
          lease_renewal_interval_in_ms: pos_integer()
        }
  defstruct node: nil,
            cluster: nil,
            path_to_descriptor: nil,
            descriptor: nil,
            coordinator: :unavailable,
            director: :unavailable,
            timers: nil,
            missed_pongs: 0,
            mode: :active,
            capabilities: [],
            transaction_system_layout: nil,
            storage_table: nil,
            #
            deadline_by_version: %{},
            minimum_read_version: nil,
            lease_renewal_interval_in_ms: 5_000
end
