defmodule Bedrock.Cluster.Gateway.State do
  @moduledoc false

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator

  @type t :: %__MODULE__{
          node: node(),
          cluster: module(),
          path_to_descriptor: Path.t(),
          descriptor: Descriptor.t(),
          known_coordinator: Coordinator.ref() | :unavailable,
          timers: %{atom() => reference()} | nil,
          mode: :passive | :active,
          capabilities: [Bedrock.Cluster.capability()],
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          #
          deadline_by_version: %{Bedrock.version() => Bedrock.timestamp_in_ms()},
          minimum_read_version: Bedrock.version() | nil,
          lease_renewal_interval_in_ms: pos_integer()
        }
  defstruct node: nil,
            cluster: nil,
            path_to_descriptor: nil,
            descriptor: nil,
            known_coordinator: :unavailable,
            timers: nil,
            mode: :active,
            capabilities: [],
            transaction_system_layout: nil,
            #
            deadline_by_version: %{},
            minimum_read_version: nil,
            lease_renewal_interval_in_ms: 5_000
end
