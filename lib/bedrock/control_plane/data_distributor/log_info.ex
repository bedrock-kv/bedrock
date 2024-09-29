defmodule Bedrock.ControlPlane.DataDistributor.LogInfo do
  @moduledoc """
  """

  @typedoc """
  A `LogInfo` struct is used to store basic information about a `TransactionLog`.
  """
  @type t :: %__MODULE__{
          id: id(),
          tag: tag(),
          endpoint: endpoint()
        }
  defstruct id: nil,
            tag: nil,
            endpoint: nil

  alias Bedrock.DataPlane.TransactionLog

  @type id :: TransactionLog.id()
  @type tag :: integer()
  @type endpoint :: pid() | atom()

  @spec new(id(), tag(), endpoint()) :: t()
  def new(id, tag, endpoint) do
    %__MODULE__{
      id: id,
      tag: tag,
      endpoint: endpoint
    }
  end
end
