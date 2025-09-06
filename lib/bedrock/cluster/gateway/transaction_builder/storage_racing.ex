defmodule Bedrock.Cluster.Gateway.TransactionBuilder.StorageRacing do
  @moduledoc """
  Storage server racing and caching utilities.

  This module provides infrastructure for racing multiple storage servers
  to find the fastest one and caching the results for future use.

  The racing pattern allows any operation to be performed across multiple
  storage servers in parallel, returning the first successful result along
  with the winning server for future caching.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  @doc """
  Race storage servers for a key, using caching transparently.

  Uses the key to determine the storage servers and check for cached fastest server.
  If no cached server exists, races all servers and caches the winner.

  ## Parameters
  - `key`: The key to look up storage servers and cache by
  - `state`: Transaction builder state with layout and cache
  - `operation_fn`: Function to run `(pid(), State.t() -> {:ok, any()} | {:error, any()})`
  - `opts`: Options (currently unused but kept for compatibility)

  ## Returns
  - `{:ok, {result, key_range}, updated_state}` - Result with shard key range and potentially updated cache
  - `{:error, reason, state}` - All servers failed or unavailable
  """
  @spec race_storage_servers(
          state :: State.t(),
          key :: binary(),
          operation_fn :: (pid(), State.t() -> {:ok, any()} | {:error, any()}),
          opts :: keyword()
        ) ::
          {:ok, {any(), Bedrock.key_range()}, State.t()}
          | {:error, :timeout, State.t()}
          | {:error, :unavailable, State.t()}
  def race_storage_servers(%State{} = state, key, operation_fn, opts \\ []) do
    case LayoutIndex.lookup_key!(state.layout_index, key) do
      {key_range, storage_pids} when storage_pids != [] ->
        race_and_save_winner(key_range, storage_pids, state, operation_fn, opts)

      {_key_range, []} ->
        {:error, :unavailable, state}
    end
  rescue
    RuntimeError -> {:error, :unavailable, state}
  end

  defp race_and_save_winner(key_range, storage_pids, state, operation_fn, opts) do
    with :error <- Map.fetch(state.fastest_storage_servers, key_range),
         {:ok, winning_server, result} <- run_race(storage_pids, state, operation_fn, opts) do
      updated_state = %{
        state
        | fastest_storage_servers: Map.put(state.fastest_storage_servers, key_range, winning_server)
      }

      {:ok, {result, key_range}, updated_state}
    else
      {:ok, fastest_server} ->
        case operation_fn.(fastest_server, state) do
          {:ok, result} -> {:ok, {result, key_range}, state}
          {:error, reason} -> {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  # Find the best result based on priority:
  # 1. Any success result (short-circuits immediately)
  # 2. Any meaningful error (not timeout/unsupported)
  # 3. Timeout or unsupported
  # 4. No results -> unavailable
  defp run_race(storage_pids, state, operation_fn, _opts) do
    storage_pids
    |> Task.async_stream(
      fn storage_server ->
        case operation_fn.(storage_server, state) do
          {:ok, result} -> {:ok, storage_server, result}
          {:error, _} = error -> error
        end
      end,
      ordered: false,
      timeout: state.fetch_timeout_in_ms
    )
    |> Stream.map(&elem(&1, 1))
    |> determine_winner()
  end

  def determine_winner(stream), do: stream |> collect_results() |> determine_outcome()

  defp collect_results(stream), do: Enum.reduce_while(stream, {nil, [], []}, &process_result/2)

  defp process_result({:ok, storage_server, result}, _acc), do: {:halt, {{:ok, storage_server, result}, [], []}}

  defp process_result({:error, reason} = error, {success, meaningful_errors, fallback_errors}) do
    if fallback_error?(reason) do
      {:cont, {success, meaningful_errors, [error | fallback_errors]}}
    else
      {:cont, {success, [error | meaningful_errors], fallback_errors}}
    end
  end

  defp process_result({:exit, :timeout}, {success, meaningful_errors, fallback_errors}),
    do: {:cont, {success, meaningful_errors, [{:error, :timeout} | fallback_errors]}}

  defp process_result(_other, acc), do: {:cont, acc}

  def determine_outcome({success, meaningful_errors, fallback_errors}),
    do: success || best_error(meaningful_errors, fallback_errors)

  defp fallback_error?(reason) when reason in [:timeout, :unsupported], do: true
  defp fallback_error?(_), do: false

  defp best_error([error | _], _), do: error
  defp best_error(_, []), do: {:error, :unavailable}
  defp best_error(_, fallback_errors), do: Enum.find(fallback_errors, &timeout_error?/1) || List.first(fallback_errors)

  defp timeout_error?({:error, :timeout}), do: true
  defp timeout_error?(_), do: false
end
