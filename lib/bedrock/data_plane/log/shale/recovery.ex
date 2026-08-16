defmodule Bedrock.DataPlane.Log.Shale.Recovery do
  @moduledoc """
  Recovery logic for Shale log servers.

  Every log stores the same encoded transaction stream. Recovery pulls from one
  available survivor, appends each source binary unchanged, and lets the fresh
  destination Demux perform normal shard slicing.
  """
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 4]

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.DemuxControl
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction

  @spec recover_from(
          State.t(),
          source_logs :: [Log.ref()],
          replay_after :: Bedrock.version(),
          last_inclusive :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | {:error, :lock_required}
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
          | {:error, :no_source_logs_available}
          | {:error, {:incomplete_replay, Bedrock.version(), Bedrock.version()}}
          | {:error, term(), State.t()}
  def recover_from(t, _, _, _) when t.mode != :locked, do: {:error, :lock_required}

  def recover_from(t, _source_logs, replay_after, last_inclusive) when replay_after > last_inclusive,
    do: {:error, :invalid_version_range, t}

  def recover_from(t, source_logs, replay_after, last_inclusive) do
    t = prepare_replay(t, replay_after)

    result =
      if replay_after == last_inclusive do
        persist_empty_baseline(t, replay_after)
      else
        pull_transactions_from_sources(t, source_logs, replay_after, last_inclusive)
      end

    case result do
      {:ok, %{last_version: ^last_inclusive} = t} -> {:ok, %{t | mode: :running}}
      {:ok, t} -> {:error, {:incomplete_replay, t.last_version, last_inclusive}, lock_failed_replay(t)}
      {:error, reason, t} -> {:error, reason, lock_failed_replay(t)}
    end
  end

  defp lock_failed_replay(t) do
    t = close_writer(t)
    DemuxControl.teardown(t.demux)
    %{t | mode: :locked, demux: nil}
  end

  defp prepare_replay(t, replay_after) do
    t
    |> Map.put(:mode, :recovering)
    |> abort_all_waiting_pullers()
    |> abort_all_pending_pushes()
    |> close_writer()
    |> discard_all_segments()
    |> Map.merge(%{
      active_segment: nil,
      segments: [],
      writer: nil,
      available_after: replay_after,
      oldest_version: replay_after,
      last_version: replay_after,
      pending_pushes: %{}
    })
    |> reset_demux()
  end

  defp persist_empty_baseline(t, replay_after) do
    case Segment.allocate_from_recycler(t.segment_recycler, t.path, replay_after, replay_after) do
      {:ok, segment} ->
        persist_allocated_baseline(t, segment, replay_after)

      {:error, reason} ->
        {:error, {:unable_to_persist_replay_cursor, reason}, t}
    end
  end

  defp persist_allocated_baseline(t, segment, replay_after) do
    case Writer.open(segment.path, replay_after) do
      {:ok, writer} ->
        case Writer.sync(writer) do
          :ok ->
            :ok = Writer.close(writer)

            {:ok,
             %{
               t
               | active_segment: %{segment | transactions: []},
                 writer: nil
             }}

          {:error, reason} ->
            _ = Writer.close(writer)
            :ok = Segment.return_to_recycler(segment, t.segment_recycler)
            {:error, {:unable_to_persist_replay_cursor, reason}, t}
        end

      {:error, reason} ->
        :ok = Segment.return_to_recycler(segment, t.segment_recycler)
        {:error, {:unable_to_persist_replay_cursor, reason}, t}
    end
  end

  @doc """
  Tears down the previous Demux incarnation (synchronously, so no stale
  buffer or in-flight flush can write a chunk afterward) and starts a fresh
  one. The durability floor resets to nil: it re-derives from fresh
  confirmations only, pinning trim at the recovery durable version until the
  replayed range re-confirms.

  States without an object storage backend (segment-only unit tests) are
  left untouched.
  """
  @spec reset_demux(State.t()) :: State.t()
  def reset_demux(%{object_storage: nil} = t), do: t

  def reset_demux(t) do
    DemuxControl.teardown(t.demux)

    case DemuxControl.start(t) do
      {:ok, demux} -> %{t | demux: demux, min_durable_version: nil}
      {:error, reason} -> raise "Failed to start demux for recovery: #{inspect(reason)}"
    end
  end

  # No chunk cleanup pass: cut broadcasts are gated on the known-committed
  # version, so chunks can never contain versions a recovery would discard.
  # Deterministic replay re-produces byte-identical chunks, and
  # `:already_exists` is always a truthful confirmation.

  @spec pull_transactions_from_sources(
          t :: State.t(),
          source_logs :: [Log.ref()],
          replay_after :: Bedrock.version(),
          last_inclusive :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | Log.pull_errors()
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
          | {:error, :no_source_logs_available}
          | {:error, term(), State.t()}

  def pull_transactions_from_sources(t, [], _replay_after, _last_inclusive), do: {:error, :no_source_logs_available, t}

  # Single source log - use original behavior
  def pull_transactions_from_sources(t, [source_log], replay_after, last_inclusive) do
    pull_transactions(t, source_log, replay_after, last_inclusive)
  end

  # Multiple source logs - try each in order until one succeeds
  # All logs have the same version sequence, so any survivor works
  def pull_transactions_from_sources(t, source_logs, replay_after, last_inclusive) do
    try_pull_from_sources(t, source_logs, replay_after, last_inclusive, [])
  end

  defp try_pull_from_sources(t, [], _replay_after, _last_inclusive, errors) do
    # All sources failed, return the last error
    case errors do
      [reason | _] -> {:error, reason, t}
      _ -> {:error, :no_source_logs_available, t}
    end
  end

  defp try_pull_from_sources(t, [source_log | rest], replay_after, last_inclusive, errors) do
    case pull_transactions(t, source_log, replay_after, last_inclusive) do
      {:ok, t} ->
        {:ok, t}

      {:error, {:source_log_unavailable, _} = reason, t} ->
        # A source may disappear between pages. Reset the partial destination
        # before trying another survivor so bytes from attempts cannot mix.
        t = prepare_replay(t, replay_after)
        try_pull_from_sources(t, rest, replay_after, last_inclusive, [reason | errors])

      {:error, _reason, _t} = error ->
        error
    end
  end

  @spec pull_transactions(
          t :: State.t(),
          log_ref :: Log.ref(),
          replay_after :: Bedrock.version(),
          last_inclusive :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | Log.pull_errors()
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
          | {:error, term(), State.t()}
  def pull_transactions(t, _, replay_after, last_inclusive) when replay_after == last_inclusive, do: {:ok, t}

  def pull_transactions(t, log_ref, replay_after, last_inclusive) do
    case Log.pull(log_ref, replay_after, recovery: true, last_version: last_inclusive) do
      {:ok, []} ->
        {:error, {:incomplete_replay, replay_after, last_inclusive}, t}

      {:ok, transactions} ->
        transactions
        |> Enum.reduce_while({replay_after, t}, fn bytes, acc ->
          process_transaction_bytes(bytes, acc, last_inclusive)
        end)
        |> case do
          {:error, _reason, _t} = error -> error
          {^last_inclusive, t} -> {:ok, t}
          {next_replay_after, t} -> pull_transactions(t, log_ref, next_replay_after, last_inclusive)
        end

      {:error, :unavailable} ->
        {:error, {:source_log_unavailable, log_ref}, t}

      {:error, reason} ->
        {:error, {:log_pull_failed, reason, log_ref}, t}
    end
  end

  @spec process_transaction_bytes(Transaction.encoded(), {Bedrock.version(), State.t()}, Bedrock.version()) ::
          {:cont, {Bedrock.version(), State.t()}} | {:halt, {:error, term(), State.t()}}
  defp process_transaction_bytes(bytes, {cursor, t}, last_inclusive) do
    case Transaction.commit_version(bytes) do
      {:ok, version} when is_binary(version) ->
        process_versioned_transaction(bytes, version, cursor, last_inclusive, t)

      {:ok, nil} ->
        {:halt, {:error, :missing_transaction_id, t}}

      {:error, :invalid_format} ->
        {:halt, {:error, :invalid_transaction, t}}

      {:error, reason} ->
        {:halt, {:error, reason, t}}
    end
  end

  defp process_versioned_transaction(bytes, version, cursor, last_inclusive, t)
       when version > cursor and version <= last_inclusive,
       do: handle_valid_transaction_bytes(bytes, version, cursor, t)

  defp process_versioned_transaction(_bytes, version, _cursor, last_inclusive, t) when version > last_inclusive,
    do: {:halt, {:error, {:transaction_beyond_replay_endpoint, version, last_inclusive}, t}}

  defp process_versioned_transaction(_bytes, version, cursor, _last_inclusive, t),
    do: {:halt, {:error, {:transaction_not_after_cursor, version, cursor}, t}}

  @spec handle_valid_transaction_bytes(
          Transaction.encoded(),
          Bedrock.version(),
          Bedrock.version(),
          State.t()
        ) ::
          {:cont, {Bedrock.version(), State.t()}} | {:halt, {:error, term(), State.t()}}
  defp handle_valid_transaction_bytes(bytes, version, cursor, t) do
    with {:ok, _transaction} <- Transaction.decode(bytes),
         {:ok, t, [^bytes]} <- push(t, cursor, bytes, fn _ -> :ok end) do
      # Replay routes through the fresh Demux so the replayed range re-enters
      # the chunk pipeline and re-confirms deterministically.
      push_to_demux(t, version, bytes)
      {:cont, {version, t}}
    else
      {:wait, t} -> {:halt, {:error, :tx_out_of_order, t}}
      {:error, :invalid_format} -> {:halt, {:error, :invalid_transaction, t}}
      {:error, reason, t, _appended_transactions} -> {:halt, {:error, reason, t}}
      {:error, reason} -> {:halt, {:error, reason, t}}
    end
  end

  defp push_to_demux(%{demux: nil}, _version, _bytes), do: :ok

  # Everything replayed is committed by definition (recovery only replays
  # the committed range, in order), so each replayed version doubles as its
  # own known-committed watermark — cuts fire during replay.
  defp push_to_demux(t, version, bytes), do: Demux.Server.push(t.demux, version, bytes, version)

  @spec abort_all_waiting_pullers(State.t()) :: State.t()
  def abort_all_waiting_pullers(%{waiting_pullers: waiting_pullers} = t) do
    Enum.reduce(waiting_pullers, %{t | waiting_pullers: %{}}, fn {_version, puller_list}, t ->
      Enum.each(puller_list, fn {_timestamp, reply_to_fn, _opts} ->
        reply_to_fn.({:ok, []})
      end)

      t
    end)
  end

  @spec abort_all_pending_pushes(State.t()) :: State.t()
  def abort_all_pending_pushes(%{pending_pushes: pending_pushes} = t) do
    Enum.each(pending_pushes, fn {_version, {_transaction, ack_fn}} ->
      :ok = ack_fn.({:error, :not_ready})
    end)

    %{t | pending_pushes: %{}}
  end

  @spec close_writer(State.t()) :: State.t()
  def close_writer(%{writer: nil} = t), do: t

  @spec close_writer(State.t()) :: State.t()
  def close_writer(%{writer: writer} = t) do
    :ok = Writer.close(writer)
    %{t | writer: nil}
  end

  @spec discard_all_segments(State.t()) :: State.t()
  def discard_all_segments(%{active_segment: nil, segments: segments} = t),
    do: %{t | segments: discard_segments(t.segment_recycler, segments)}

  @spec discard_all_segments(State.t()) :: State.t()
  def discard_all_segments(%{active_segment: active_segment, segments: segments} = t) do
    %{
      t
      | active_segment: nil,
        segments: discard_segments(t.segment_recycler, [active_segment | segments])
    }
  end

  @spec discard_segments(term(), [Segment.t()]) :: []
  def discard_segments(_segment_recycler, []), do: []

  @spec discard_segments(term(), [Segment.t()]) :: []
  def discard_segments(segment_recycler, [segment | remaining_segments]) do
    :ok = SegmentRecycler.check_in(segment_recycler, segment.path)
    discard_segments(segment_recycler, remaining_segments)
  end
end
