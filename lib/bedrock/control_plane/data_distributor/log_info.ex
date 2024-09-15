defmodule Bedrock.ControlPlane.DataDistributor.LogInfo do
  alias Bedrock.Service.TransactionLog
  alias Bedrock.ControlPlane.DataDistributor

  @type t :: %__MODULE__{
          id: TransactionLog.id(),
          tag: DataDistributor.tag(),
          endpoint: DataDistributor.endpoint()
        }

  defstruct ~w[id tag endpoint]a

  def new(id, tag, endpoint) do
    %__MODULE__{
      id: id,
      tag: tag,
      endpoint: endpoint
    }
  end
end
