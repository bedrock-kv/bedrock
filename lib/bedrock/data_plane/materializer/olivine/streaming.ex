defmodule Bedrock.DataPlane.Materializer.Olivine.Streaming do
  @moduledoc """
  The materializer's stream puller: one loop, from snapshot to live.

  Pulls this materializer's shard stream from a Demux ShardServer — chunks
  for history, buffer for recent data, seamlessly — instead of drinking the
  whole WAL from the log. The materializer's only contact with a log is
  discovery: `Log.get_shard_server/2`, once per session (and again on
  failover).

  ## Discovery

  The replica set arrives in the unlock seed: `[{log_id, log_ref}]`,
  resolved once by the director through the same `ShardRouter` walk the
  commit proxies route with (`ShardRouter.log_ids_for_tag/3`) — the
  puller never re-derives placement. Any replica works: every replica
  sees the same slices and the same cuts, so their chunks are
  byte-identical and their buffers hold the same suffix. Failover walks
  the replica set with the same circuit-breaker pattern the old log
  puller used.

  ## Currency

  An empty pull reply that carries a high-water — "nothing for you, but you
  are current through v" — is rematerialized as a heartbeat: an empty
  transaction at v fed through the normal apply path. Version advancement,
  read wake-ups, and window math all ride existing code, unchanged.

  ## Backpressure

  Batches are handed to the owner through a synchronous ingest function
  whose reply the owner withholds while its intake queue is over high-water.
  The puller cannot outrun the applier because the applier holds the reply.
  """
  import Bedrock.DataPlane.Materializer.Telemetry

  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @type ingest_fn :: ([Transaction.encoded()], kcv :: Bedrock.version() | nil -> :ok)

  @type source :: {Log.id(), Log.ref()}

  @type puller_state :: %{
          shard_num: non_neg_integer(),
          next_version: Bedrock.version(),
          sources: [source()],
          failed_logs: %{Log.id() => integer()},
          shard_server: pid() | nil,
          current_log_id: Log.id() | nil,
          ingest_fn: ingest_fn()
        }

  @spec start_pulling(
          shard_num :: non_neg_integer(),
          start_after :: Bedrock.version(),
          sources :: [source()],
          ingest_fn()
        ) :: Task.t()
  def start_pulling(shard_num, start_after, sources, ingest_fn) do
    state = %{
      shard_num: shard_num,
      next_version: Version.increment(start_after),
      sources: sources,
      failed_logs: %{},
      shard_server: nil,
      current_log_id: nil,
      ingest_fn: ingest_fn
    }

    Task.async(fn -> stream_loop(state) end)
  end

  @spec stop(Task.t()) :: :ok
  def stop(puller) do
    Task.shutdown(puller)
    :ok
  end

  @spec circuit_breaker_timeout() :: pos_integer()
  def circuit_breaker_timeout, do: 10_000

  @spec retry_delay() :: pos_integer()
  def retry_delay, do: 1_000

  @spec call_timeout() :: pos_integer()
  def call_timeout, do: 5_000

  @spec pull_limit() :: pos_integer()
  def pull_limit, do: 100

  @spec stream_loop(puller_state()) :: no_return()
  def stream_loop(state) do
    case ensure_shard_server(state) do
      {:ok, state} ->
        state |> pull_once() |> stream_loop()

      {:wait, state} ->
        trace_log_pull_circuit_breaker_tripped(state.next_version, retry_delay())
        :timer.sleep(retry_delay())
        state |> reset_failed_logs() |> stream_loop()
    end
  end

  @spec pull_once(puller_state()) :: puller_state()
  def pull_once(state) do
    trace_log_pull_start(state.next_version, state.next_version)

    case safe_pull(state.shard_server, state.next_version) do
      {:ok, [], %{high_water: high_water, kcv: kcv}} when high_water >= state.next_version ->
        # Nothing for this shard, but the cluster is current through the
        # high-water: rematerialize the heartbeat and advance.
        :ok = state.ingest_fn.([heartbeat_at(high_water)], kcv)
        %{state | next_version: Version.increment(high_water)}

      {:ok, [], _currency} ->
        state

      {:ok, transactions, %{kcv: kcv}} ->
        trace_log_pull_succeeded(state.next_version, length(transactions))
        slices = Enum.map(transactions, fn {_version, slice} -> slice end)
        {last_version, _} = List.last(transactions)
        :ok = state.ingest_fn.(slices, kcv)
        %{state | next_version: Version.increment(last_version)}

      {:error, :timeout} ->
        # A quiet long-poll window: same server, ask again.
        state

      {:error, reason} ->
        trace_log_pull_failed(state.next_version, reason)
        fail_over(state)
    end
  end

  @spec ensure_shard_server(puller_state()) :: {:ok, puller_state()} | {:wait, puller_state()}
  def ensure_shard_server(%{shard_server: pid} = state) when is_pid(pid), do: {:ok, state}

  def ensure_shard_server(state) do
    case select_log(state) do
      {:ok, {log_id, worker}} ->
        case safe_get_shard_server(worker, state.shard_num) do
          {:ok, pid} ->
            {:ok, %{state | shard_server: pid, current_log_id: log_id}}

          {:error, _reason} ->
            ensure_shard_server(mark_log_as_failed(state, log_id))
        end

      :no_available_logs ->
        {:wait, state}
    end
  end

  # Select a replica for this shard, excluding those with active circuit breakers
  @spec select_log(puller_state()) ::
          {:ok, source()} | :no_available_logs
  def select_log(%{sources: sources, failed_logs: failed_logs}) do
    now = System.monotonic_time(:millisecond)

    sources
    |> Enum.filter(fn {log_id, _ref} ->
      case Map.get(failed_logs, log_id) do
        nil -> true
        retry_timestamp -> now >= retry_timestamp
      end
    end)
    |> case do
      [] -> :no_available_logs
      available -> {:ok, Enum.random(available)}
    end
  end

  @spec mark_log_as_failed(puller_state(), Log.id()) :: puller_state()
  def mark_log_as_failed(state, log_id) do
    retry_timestamp = System.monotonic_time(:millisecond) + circuit_breaker_timeout()
    trace_log_marked_as_failed(state.next_version, log_id)
    %{state | failed_logs: Map.put(state.failed_logs, log_id, retry_timestamp)}
  end

  @spec reset_failed_logs(puller_state()) :: puller_state()
  def reset_failed_logs(state) do
    trace_log_pull_circuit_breaker_reset(state.next_version)
    %{state | failed_logs: %{}}
  end

  defp fail_over(%{current_log_id: nil} = state), do: %{state | shard_server: nil}

  defp fail_over(state) do
    state
    |> mark_log_as_failed(state.current_log_id)
    |> Map.merge(%{shard_server: nil, current_log_id: nil})
  end

  defp safe_pull(shard_server, from_version) do
    ShardServer.pull(shard_server, from_version, timeout: call_timeout(), limit: pull_limit())
  catch
    :exit, _ -> {:error, :unavailable}
  end

  defp safe_get_shard_server(log_worker, shard_num) do
    Log.get_shard_server(log_worker, shard_num)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  # The heartbeat, rematerialized at the shard boundary: an empty
  # transaction at the high-water, fed through the normal apply path.
  @spec heartbeat_at(Bedrock.version()) :: Transaction.encoded()
  def heartbeat_at(version) do
    Transaction.encode(%{mutations: [], commit_version: version})
  end
end
