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
          director: {Bedrock.epoch(), director :: pid()} | :unavailable,
          timers: map() | nil,
          missed_pongs: non_neg_integer(),
          mode: :passive | :active,
          capabilities: [Bedrock.Cluster.capability()],
          tranasction_system_layout: TransactionSystemLayout.t() | nil,
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
            tranasction_system_layout: nil,
            capabilities: [],
            #
            deadline_by_version: %{},
            minimum_read_version: nil,
            lease_renewal_interval_in_ms: 5_000
end
