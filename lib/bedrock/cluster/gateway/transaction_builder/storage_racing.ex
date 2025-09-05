defmodule Bedrock.Cluster.Gateway.TransactionBuilder.StorageRacing do
  @moduledoc """
  Storage server racing and caching utilities.

  This module provides infrastructure for racing multiple storage servers
  to find the fastest one and caching the results for future use.

  The racing pattern allows any operation to be performed across multiple
  storage servers in parallel, returning the first successful result along
  with the winning server for future caching.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutUtils
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  @type async_stream_fn() :: (list(), function(), keyword() -> Enumerable.t())

  @doc """
  Race storage servers for a key, using caching transparently.

  Uses the key to determine the storage servers and check for cached fastest server.
  If no cached server exists, races all servers and caches the winner.

  ## Parameters
  - `key`: The key to look up storage servers and cache by
  - `state`: Transaction builder state with layout and cache
  - `operation_fn`: Function to run `(pid(), State.t() -> {:ok, any()} | {:error, any()})`
  - `opts`: Options including async_stream_fn for testing

  ## Returns
  - `{:ok, result, updated_state}` - Result with potentially updated cache
  - `{:error, reason, state}` - All servers failed or unavailable
  """
  @spec race_storage_servers(
          state :: State.t(),
          key :: binary(),
          operation_fn :: (pid(), State.t() -> {:ok, any()} | {:error, any()}),
          opts :: [async_stream_fn: async_stream_fn()]
        ) ::
          {:ok, any(), State.t()} | {:error, :timeout, State.t()} | {:error, :unavailable, State.t()}
  def race_storage_servers(%State{} = state, key, operation_fn, opts \\ []) do
    case LayoutUtils.storage_servers_for_key(state.layout_index, key) do
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

      {:ok, result, updated_state}
    else
      {:ok, fastest_server} ->
        case operation_fn.(fastest_server, state) do
          {:ok, result} -> {:ok, result, state}
          {:error, reason} -> {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp run_race(storage_pids, state, operation_fn, opts) do
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)

    storage_pids
    |> async_stream_fn.(
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
    |> Enum.reduce_while(nil, fn
      {:ok, storage_server, result}, _acc -> {:halt, {:ok, storage_server, result}}
      {:error, :unsupported}, acc -> {:cont, acc}
      {:error, _} = error, nil -> {:cont, error}
      {:error, _}, acc -> {:cont, acc}
      {:exit, :timeout}, nil -> {:cont, {:error, :timeout}}
      {:exit, :timeout}, acc -> {:cont, acc}
    end) || {:error, :unavailable}
  end
end
