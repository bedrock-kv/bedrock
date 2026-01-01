defmodule Bedrock.ControlPlane.Config.ResolverDescriptor do
  @moduledoc """
  Describes a resolver's configuration in the transaction system layout.
  """

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
