defmodule Bedrock.ControlPlane.Config.ResolverDescriptor do
  @moduledoc false

  @type t :: %{
          start_key: Bedrock.key(),
          resolver: pid() | nil
        }

  @spec resolver_descriptor(start_key :: Bedrock.key(), resolver :: pid() | nil) :: t()
  def resolver_descriptor(start_key, resolver),
    do: %{
      start_key: start_key,
      resolver: resolver
    }
end
