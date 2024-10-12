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
          timer_ref: reference() | nil,
          mode: :passive | :active,
          capabilities: [Bedrock.Cluster.capability()]
        }
  defstruct node: nil,
            cluster: nil,
            path_to_descriptor: nil,
            descriptor: nil,
            coordinator: :unavailable,
            controller: :unavailable,
            timer_ref: nil,
            mode: :active,
            capabilities: []
end
