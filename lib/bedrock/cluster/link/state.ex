defmodule Bedrock.Cluster.Link.State do
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
          coordinator_monitor: reference() | nil,
          timers: %{atom() => reference()} | nil,
          mode: :passive | :active,
          capabilities: [Bedrock.Cluster.capability()],
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          wiring_epoch: Bedrock.epoch() | nil,
          routing_table: :ets.table()
        }
  defstruct node: nil,
            cluster: nil,
            path_to_descriptor: nil,
            descriptor: nil,
            known_coordinator: :unavailable,
            coordinator_monitor: nil,
            timers: nil,
            mode: :active,
            capabilities: [],
            transaction_system_layout: nil,
            # High-water mark of the wiring we have seen. Separate from
            # the cached layout because a clear drops the layout without
            # lowering the mark.
            wiring_epoch: nil,
            routing_table: nil
end
