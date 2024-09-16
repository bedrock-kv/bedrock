defmodule Bedrock.ControlPlane.DataDistributor.LogInfo do
  alias Bedrock.Service.TransactionLog

  @type t :: %__MODULE__{
          id: id(),
          tag: tag(),
          endpoint: endpoint()
        }

  @type id :: TransactionLog.id()
  @type tag :: integer()
  @type endpoint :: pid() | atom()

  defstruct ~w[id tag endpoint]a

  @spec new(id(), tag(), endpoint()) :: t()
  def new(id, tag, endpoint) do
    %__MODULE__{
      id: id,
      tag: tag,
      endpoint: endpoint
    }
  end
end
