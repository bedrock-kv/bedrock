defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @type id :: String.t()
  @type type :: Bedrock.service()

  @type t :: %__MODULE__{
          id: id(),
          type: type(),
          pid: pid()
        }
  defstruct id: nil,
            type: nil,
            pid: nil

  @spec new(id(), type(), pid()) :: t()
  def new(id, type, pid) do
    %__MODULE__{
      id: id,
      type: type,
      pid: pid
    }
  end
end
