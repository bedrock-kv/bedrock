defmodule Bedrock.DataPlane.CommitProxy.LayoutOptimization do
  @moduledoc """
  Precomputes expensive static structures from TransactionSystemLayout to optimize
  per-batch transaction processing performance.
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @max_key "\xff\xff\xff"

  @type precomputed_layout :: %{
          resolver_ends: [{Bedrock.key(), pid()}],
          resolver_refs: [pid()]
        }
  @spec precompute_from_layout(TransactionSystemLayout.t()) :: precomputed_layout()
  def precompute_from_layout(%{resolvers: resolvers} = _layout) do
    resolver_list = resolvers
    resolver_ends = calculate_resolver_ends_optimized(resolver_list)
    resolver_refs = Enum.map(resolver_list, &elem(&1, 1))

    %{
      resolver_ends: resolver_ends,
      resolver_refs: resolver_refs
    }
  end

  @spec calculate_resolver_ends_optimized([{Bedrock.key(), any()}]) :: [{Bedrock.key(), pid()}]
  defp calculate_resolver_ends_optimized(resolvers) do
    sorted_resolvers = Enum.sort_by(resolvers, fn {start_key, _ref} -> start_key end)

    sorted_resolvers
    |> Enum.with_index()
    |> Enum.map(fn {{_start_key, resolver_ref}, index} ->
      max_key_ex =
        if index + 1 < length(sorted_resolvers) do
          {next_start, _} = Enum.at(sorted_resolvers, index + 1)
          next_start
        else
          @max_key
        end

      {max_key_ex, resolver_ref}
    end)
  end
end
