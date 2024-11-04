defmodule Bedrock.ControlPlane.Config.TransactionResolverDescriptor do
  @type t :: %{
          key_range: Bedrock.key_range(),
          resolver: pid()
        }

  @spec new(key_range :: Bedrock.key_range(), resolver :: pid()) :: t()
  def new(key_range, resolver),
    do: %{
      key_range: key_range,
      resolver: resolver
    }
end
