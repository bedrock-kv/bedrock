defmodule Bedrock.Internal.TransactionBuilder.StorageRacing do
  @moduledoc """
  Storage server racing and caching utilities.

  This module provides infrastructure for racing multiple storage servers
  to find the fastest one and caching the results for future use.

  The racing pattern allows any operation to be performed across multiple
  storage servers in parallel, returning the first successful result along
  with the winning server for future caching.
  """

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.State

  @doc """
  Race storage servers for a key, using caching transparently.

  Uses the key to determine the storage servers and check for cached fastest server.
  If no cached server exists, races all servers and caches the winner.

  ## Parameters
  - `key`: The key to look up storage servers and cache by
  - `state`: Transaction builder state with layout and cache
  - `operation_fn`: Function to run `(pid(), Bedrock.version(), Bedrock.timeout_in_ms() -> {:ok, any()} | {:error, any()} | {:failure, atom(), pid()})`

  ## Returns
  - `{:ok, {result, key_range}, updated_state}` - Result with shard key range and potentially updated cache
  - `{:error, reason, state}` - All servers failed or unavailable
  """
  @spec race_storage_servers(
          state :: State.t(),
          key :: binary(),
          operation_fn :: (pid(), Bedrock.version(), Bedrock.timeout_in_ms() ->
                             {:ok, any()}
                             | {:error, :not_found | :version_too_new}
                             | {:failure, :timeout | :unavailable | :version_too_old, pid()})
        ) ::
          {State.t(),
           {:ok, {any(), Bedrock.key_range()}}
           | {:failure, %{atom() => [pid()]}}}
  def race_storage_servers(%State{} = state, key, operation_fn) do
    case LayoutIndex.lookup_key(state.layout_index, key) do
      {:ok, {key_range, storage_refs}} ->
        state.fastest_storage_servers
        |> Map.get(key_range)
        |> try_fastest_server(state, key_range, storage_refs, operation_fn)

      :not_cached ->
        resolve_then_race(state, key, operation_fn)
    end
  end

  # The by-key resolve-through (FDB fetches locations per read): a local
  # miss asks the routing fn for the single covering entry and caches it
  # into the builder's partial index for the transaction's lifetime.
  # Without a routing fn (caller-provided wiring, tests) resolution is
  # impossible and the read fails as layout_lookup_failed. Fetch failures
  # surface as read failures; the Repo retry loop classifies them.
  defp resolve_then_race(%State{routing_fn: nil} = state, _key, _operation_fn),
    do: {state, {:failure, %{layout_lookup_failed: []}}}

  defp resolve_then_race(%State{routing_fn: routing_fn} = state, key, operation_fn) do
    case routing_fn.(key) do
      {:ok, {start_key, end_key, [_ | _] = storage_refs}} ->
        state = %{state | layout_index: LayoutIndex.insert(state.layout_index, start_key, end_key, storage_refs)}
        try_fastest_server(nil, state, {start_key, end_key}, storage_refs, operation_fn)

      {:ok, {_start_key, _end_key, []}} ->
        {state, {:failure, %{layout_lookup_failed: []}}}

      {:error, reason} ->
        {state, {:failure, %{reason => []}}}
    end
  end

  # Private helper functions

  defp try_fastest_server(nil, state, key_range, all_servers, operation_fn),
    do: race_all_servers(state, key_range, all_servers, operation_fn)

  defp try_fastest_server(fastest_server, state, key_range, all_servers, operation_fn) do
    fastest_server
    |> operation_fn.(state.read_version, state.fetch_timeout_in_ms)
    |> case do
      {:ok, result} ->
        {state, {:ok, {result, key_range}}}

      {:error, :not_found} ->
        {state, {:ok, {nil, key_range}}}

      {:error, :version_too_old} ->
        {state, {:failure, %{version_too_old: [fastest_server]}}}

      {:error, reason} ->
        race_remaining_servers(state, key_range, all_servers, fastest_server, reason, operation_fn)

      {:failure, reason, _server} ->
        race_remaining_servers(state, key_range, all_servers, fastest_server, reason, operation_fn)
    end
  end

  # A failure on the cached-fastest server falls back to the shard's other
  # servers. When it was the only one - today's singleton refs always - the
  # read fails with that server's own reason rather than collapsing into
  # :no_servers_to_race: the shard is routed, the server just didn't answer,
  # and only a routing-shaped verdict may evict the node's routing cache.
  # FDB's locationCache is invalidated by wrong_shard_server and by
  # all_alternatives_failed (thrown only when every endpoint is known-failed,
  # LoadBalance.actor.cpp), never by a slow reply - slow is not stale.
  defp race_remaining_servers(state, key_range, all_servers, fastest_server, reason, operation_fn) do
    case :lists.delete(fastest_server, all_servers) do
      [] -> {state, {:failure, %{reason => [fastest_server]}}}
      remaining -> race_all_servers(state, key_range, remaining, operation_fn)
    end
  end

  # An empty ref list is unroutable, and routing-shaped by construction. No
  # index the builder writes can hold one (insert above is [_ | _]-guarded),
  # so this is a fallthrough that keeps an empty set out of run_race/3, which
  # would otherwise reduce to a reasonless %{}.
  defp race_all_servers(state, _key_range, [], _operation_fn), do: {state, {:failure, %{no_servers_to_race: []}}}

  defp race_all_servers(state, key_range, servers, operation_fn) do
    case run_race(servers, state, operation_fn) do
      {:ok, winning_server, result} ->
        updated_state = %{
          state
          | fastest_storage_servers: Map.put(state.fastest_storage_servers, key_range, winning_server)
        }

        {updated_state, {:ok, {result, key_range}}}

      {:error, reason} ->
        {state, {:error, reason}}

      {:failure, reasons_by_server} ->
        failures_by_reason =
          Enum.group_by(reasons_by_server, fn {_pid, reason} -> reason end, fn {pid, _reason} -> pid end)

        {state, {:failure, failures_by_reason}}
    end
  end

  defp run_race(storage_pids, state, operation_fn) do
    storage_pids
    |> Task.async_stream(
      fn storage_server ->
        case operation_fn.(storage_server, state.read_version, state.fetch_timeout_in_ms) do
          {:ok, result} -> {:ok, storage_server, result}
          {:error, :not_found} -> {:ok, storage_server, nil}
          {:error, reason} -> {:failure, reason, storage_server}
          {:failure, reason, server} -> {:failure, reason, server}
        end
      end,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, {reason, storage_server}} -> {:failure, reason, storage_server}
    end)
    |> Enum.reduce_while({:failure, %{}}, fn
      {:ok, server, result}, _ ->
        {:halt, {:ok, server, result}}

      {:failure, reason, server}, {:failure, failures} ->
        {:cont, {:failure, Map.put(failures, server, reason)}}
    end)
  end
end
