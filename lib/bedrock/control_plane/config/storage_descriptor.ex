defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @type t :: %__MODULE__{}
  defstruct [
    # The unique id of the service.
    id: nil,

    # The type of the service.
    type: nil
  ]
end
