defmodule Bedrock.ControlPlane.DataDistributor.LogInfo do
  defstruct ~w[
    id
    tag
    endpoint
  ]a

  def new(id, tag, endpoint) do
    %__MODULE__{
      id: id,
      tag: tag,
      endpoint: endpoint
    }
  end
end
