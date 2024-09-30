defmodule Bedrock.ControlPlane.Config.TransactionResolverDescriptor do
  alias Bedrock.ControlPlane.TransactionResolver

  @type t :: %__MODULE__{
          key_range: Bedrock.key_range(),
          resolver: TransactionResolver.t()
        }
  defstruct key_range: nil,
            resolver: nil

  @spec new(key_range :: Bedrock.key_range(), resolver :: TransactionResolver.t()) :: t()
  def new(key_range, resolver) do
    %__MODULE__{
      key_range: key_range,
      resolver: resolver
    }
  end
end
