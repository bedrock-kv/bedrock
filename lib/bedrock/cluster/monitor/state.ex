defmodule Bedrock.Cluster.Monitor.State do
  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator

  @type t :: %__MODULE__{
          node: node(),
          cluster: module(),
          path_to_descriptor: Path.t(),
          descriptor: Descriptor.t(),
          coordinator: Coordinator.ref() | :unavailable,
          controller: ClusterController.ref() | :unavailable,
          timers: map() | nil,
          missed_pongs: non_neg_integer(),
          mode: :passive | :active,
          capabilities: [Bedrock.Cluster.capability()]
        }
  defstruct node: nil,
            cluster: nil,
            path_to_descriptor: nil,
            descriptor: nil,
            coordinator: :unavailable,
            controller: :unavailable,
            timers: nil,
            missed_pongs: 0,
            mode: :active,
            capabilities: []

  def put_coordinator(t, coordinator), do: %{t | coordinator: coordinator}

  def put_controller(t, controller), do: %{t | controller: controller}

  def put_missed_pongs(t, missed_pongs), do: %{t | missed_pongs: missed_pongs}

  def update_missed_pongs(t, updater), do: %{t | missed_pongs: updater.(t.missed_pongs)}
end
