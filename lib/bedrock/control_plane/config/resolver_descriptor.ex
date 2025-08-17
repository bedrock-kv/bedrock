defmodule Bedrock.ControlPlane.Config.ResolverDescriptor do
  @moduledoc false

  @type t :: %{
          start_key: Bedrock.key(),
          resolver: pid() | {:vacancy, non_neg_integer()}
        }

  @spec resolver_descriptor(
          start_key :: Bedrock.key(),
          resolver :: pid() | {:vacancy, non_neg_integer()}
        ) :: t()
  def resolver_descriptor(start_key, resolver), do: %{start_key: start_key, resolver: resolver}
end
