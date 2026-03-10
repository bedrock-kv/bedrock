defmodule Bedrock.DataPlane.CommitProxy.ResolverLayout do
  @moduledoc """
  Struct variants for resolver configuration, enabling pattern matching
  to optimize single vs sharded resolver paths in transaction finalization.
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Resolver

  @max_key "\xff\xff\xff"

  defmodule Single do
    @moduledoc """
    Layout for single-resolver configuration.

    When there's only one resolver, conflict resolution can bypass sharding
    overhead and send all conflicts directly to the single resolver.
    """

    @enforce_keys [:resolver_ref]
    defstruct [:resolver_ref]

    @type t :: %__MODULE__{
            resolver_ref: Resolver.ref()
          }
  end

  defmodule Sharded do
    @moduledoc """
    Layout for sharded multi-resolver configuration.

    When multiple resolvers are configured, conflicts must be sharded across
    resolvers based on key ranges. Each resolver handles conflicts within
    its assigned key range.
    """

    @enforce_keys [:resolver_ends, :resolver_refs]
    defstruct [:resolver_ends, :resolver_refs]

    @type t :: %__MODULE__{
            resolver_ends: [{Bedrock.key(), Resolver.ref()}],
            resolver_refs: [Resolver.ref()]
          }
  end

  @type t :: Single.t() | Sharded.t()

  @doc """
  Creates a ResolverLayout from a TransactionSystemLayout.

  Returns a `Single` struct for single-resolver configurations, or a `Sharded`
  struct for multi-resolver configurations. This enables efficient pattern matching
  in the finalization pipeline.

  ## Examples

      iex> layout = %TransactionSystemLayout{resolvers: [{"", resolver_pid}]}
      iex> ResolverLayout.from_layout(layout)
      %ResolverLayout.Single{resolver_ref: resolver_pid}

      iex> layout = %TransactionSystemLayout{resolvers: [{"", pid1}, {"\\x80", pid2}]}
      iex> ResolverLayout.from_layout(layout)
      %ResolverLayout.Sharded{resolver_ends: [...], resolver_refs: [pid1, pid2]}
  """
  @spec from_layout(TransactionSystemLayout.t()) :: t()
  def from_layout(%{resolvers: [{_start_key, resolver_ref}]}) do
    %Single{resolver_ref: resolver_ref}
  end

  def from_layout(%{resolvers: resolvers}) do
    resolver_ends = calculate_resolver_ends(resolvers)
    resolver_refs = Enum.map(resolvers, &elem(&1, 1))

    %Sharded{
      resolver_ends: resolver_ends,
      resolver_refs: resolver_refs
    }
  end

  @doc """
  Calculates the end key boundaries for each resolver.

  Given a list of resolvers with their start keys, computes the exclusive end key
  for each resolver's range. The last resolver's range extends to the maximum key.

  ## Returns

  A list of `{end_key, resolver_ref}` tuples where `end_key` is the exclusive
  upper bound for that resolver's key range.
  """
  @spec calculate_resolver_ends([{Bedrock.key(), Resolver.ref()}]) :: [{Bedrock.key(), Resolver.ref()}]
  def calculate_resolver_ends(resolvers) do
    resolvers
    |> Enum.sort_by(fn {start_key, _ref} -> start_key end, :desc)
    |> Enum.reduce({[], @max_key}, fn {start_key, ref}, {acc, end_key} ->
      {[{end_key, ref} | acc], start_key}
    end)
    |> elem(0)
  end
end
