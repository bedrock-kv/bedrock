defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Fetching do
  @moduledoc false

  import Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions, only: [next_read_version: 1]

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutUtils
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Internal.Time

  @type next_read_version_fn() :: (State.t() ->
                                     {:ok, Bedrock.version(), Bedrock.interval_in_ms()}
                                     | {:error, atom()})
  @type time_fn() :: (-> integer())
  @type storage_fetch_fn() :: (pid(), binary(), Bedrock.version(), keyword() ->
                                 {:ok, binary()} | {:error, atom()})
  @type async_stream_fn() :: (list(), function(), keyword() -> Enumerable.t())
  @type horse_race_fn() :: ([pid()], Bedrock.version(), binary(), pos_integer(), keyword() ->
                              {:ok, pid(), binary()}
                              | {:error, atom()}
                              | :error)

  @doc false
  @spec do_fetch(State.t(), key :: binary()) ::
          {State.t(),
           {:ok, Bedrock.value()}
           | {:error, :not_found | :version_too_old | :version_too_new | :unavailable | :timeout}}
  @spec do_fetch(
          State.t(),
          key :: binary(),
          opts :: [
            next_read_version_fn: next_read_version_fn(),
            time_fn: time_fn(),
            storage_fetch_fn: storage_fetch_fn(),
            horse_race_fn: horse_race_fn()
          ]
        ) ::
          {State.t(),
           {:ok, Bedrock.value() | :version_too_old | :version_too_new | :not_found}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def do_fetch(t, key, opts \\ []) do
    {:ok, encoded_key} = t.key_codec.encode_key(key)

    fetch_fn = fn k, state ->
      case fetch_from_storage(state, k, opts) do
        {:ok, new_state, value} when is_binary(value) ->
          handle_binary_value(value, state, new_state)

        {:ok, new_state, reason} ->
          {{:error, reason}, new_state}

        {:error, _reason} ->
          {:error, state}
      end
    end

    {tx, result, t} = Tx.get(t.tx, encoded_key, fetch_fn, t)
    {%{t | tx: tx}, result}
  end

  defp handle_binary_value(value, state, new_state) do
    case state.value_codec.decode_value(value) do
      {:ok, decoded_value} -> {{:ok, decoded_value}, new_state}
      {:error, reason} -> {{:error, reason}, new_state}
    end
  end

  @spec fetch_from_storage(
          State.t(),
          key :: binary(),
          opts :: [
            next_read_version_fn: next_read_version_fn(),
            time_fn: time_fn(),
            storage_fetch_fn: storage_fetch_fn()
          ]
        ) ::
          {:ok, State.t(), binary() | :version_too_old | :version_too_new | :not_found}
          | {:error, :unavailable}
          | {:error, :timeout}
          | {:error, :lease_expired}
  def fetch_from_storage(%{read_version: nil} = t, key, opts) do
    next_read_version_fn = Keyword.get(opts, :next_read_version_fn, &next_read_version/1)
    time_fn = Keyword.get(opts, :time_fn, &Time.monotonic_now_in_ms/0)

    case next_read_version_fn.(t) do
      {:ok, read_version, read_version_lease_expiration_in_ms} ->
        read_version_lease_expiration =
          time_fn.() + read_version_lease_expiration_in_ms

        t
        |> Map.put(:read_version, read_version)
        |> Map.put(:read_version_lease_expiration, read_version_lease_expiration)
        |> fetch_from_storage(key, opts)

      {:error, :unavailable} ->
        raise "No read version available for fetching key: #{inspect(key)}"

      {:error, :lease_expired} ->
        {:error, :lease_expired}
    end
  end

  def fetch_from_storage(t, key, opts) do
    case LayoutUtils.storage_servers_for_key(t.layout_index, key) do
      {_key_range, []} ->
        {:error, :unavailable}

      {key_range, pids} ->
        fetch_with_available_servers(t, key, key_range, pids, opts)
    end
  end

  defp fetch_with_available_servers(t, key, key_range, pids, opts) do
    case Map.get(t.fastest_storage_servers, key_range) do
      nil ->
        # No cached server, horse race the pids for this range
        fetch_from_fastest_storage_server(pids, t, key_range, key, opts)

      storage_server ->
        fetch_from_cached_server(t, key, storage_server, opts)
    end
  end

  defp fetch_from_cached_server(t, key, storage_server, opts) do
    storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &Storage.fetch/4)

    case storage_fetch_fn.(storage_server, key, t.read_version, timeout: t.fetch_timeout_in_ms) do
      {:ok, value} -> {:ok, t, value}
      {:error, reason} when reason in [:not_found, :version_too_old, :version_too_new] -> {:ok, t, reason}
      error -> error
    end
  end

  @spec fetch_from_fastest_storage_server(
          storage_servers :: [pid()],
          State.t(),
          key_range :: Bedrock.key_range(),
          key :: binary(),
          opts :: [
            horse_race_fn: horse_race_fn()
          ]
        ) ::
          {:ok, State.t(), binary() | :version_too_old | :version_too_new | :not_found}
          | {:error, :timeout}
          | {:error, :unavailable}
  def fetch_from_fastest_storage_server(storage_servers, t, key_range, key, opts) do
    horse_race_fn = Keyword.get(opts, :horse_race_fn, &horse_race_storage_servers/5)

    storage_servers
    |> horse_race_fn.(t.read_version, key, t.fetch_timeout_in_ms, opts)
    |> case do
      {:ok, storage_server, result} ->
        {:ok, %{t | fastest_storage_servers: Map.put(t.fastest_storage_servers, key_range, storage_server)}, result}

      error ->
        error
    end
  end

  @doc """
  Performs a "horse race" across multiple storage servers to fetch the value
  for a given key. All of the storage servers are queried in parallel, and the
  first successful response is returned. If none of the servers return a value
  within the specified timeout, `{:error, :unavailable}` is returned.
  """
  @spec horse_race_storage_servers(
          storage_servers :: [pid()],
          read_version :: non_neg_integer(),
          key :: binary(),
          fetch_timeout_in_ms :: pos_integer(),
          opts :: [
            async_stream_fn: async_stream_fn(),
            storage_fetch_fn: storage_fetch_fn()
          ]
        ) ::
          {:ok, pid(), binary() | :version_too_old | :version_too_new | :not_found}
          | {:error, :timeout}
          | {:error, :unavailable}
  def horse_race_storage_servers(storage_servers, read_version, key, fetch_timeout_in_ms, opts) do
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &Storage.fetch/4)

    storage_servers
    |> async_stream_fn.(
      fn storage_server ->
        storage_server
        |> storage_fetch_fn.(key, read_version, timeout: fetch_timeout_in_ms)
        |> case do
          {:ok, value} ->
            {:ok, storage_server, value}

          {:error, reason} when reason in [:not_found, :version_too_old, :version_too_new] ->
            {:ok, storage_server, reason}

          error ->
            error
        end
      end,
      ordered: false,
      timeout: fetch_timeout_in_ms
    )
    |> Stream.map(&elem(&1, 1))
    |> Enum.reduce_while(nil, fn
      {:ok, _storage_server, _result} = winner, _acc -> {:halt, winner}
      {:error, :unsupported}, acc -> {:cont, acc}
      {:error, _} = error, nil -> {:cont, error}
      {:error, _}, acc -> {:cont, acc}
      {:exit, :timeout}, _acc -> {:halt, {:error, :timeout}}
    end) || {:error, :unavailable}
  end
end
