defmodule Bedrock.DataPlane.Log.Shale.Recovery do
  @moduledoc """
  Recovery logic for Shale log servers.

  Every log stores the same encoded transaction stream. Recovery pulls from one
  available survivor, appends each source binary unchanged, and lets the fresh
  destination Demux perform normal shard slicing.
  """
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [append_transaction: 2]

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.ColdStarting
  alias Bedrock.DataPlane.Log.Shale.DemuxControl
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.RecoveryAuthority

  @doc false
  @spec recover_from(State.t(), [Log.ref()], Bedrock.version(), Bedrock.version()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def recover_from(%{recovery_authority: nil} = t, _source_logs, _replay_after, _last_inclusive),
    do: {:error, :invalid_recovery_authority, t}

  def recover_from(t, source_logs, replay_after, last_inclusive),
    do: recover_from(t, t.recovery_authority, source_logs, replay_after, last_inclusive)

  @spec recover_from(
          State.t(),
          RecoveryAuthority.input(),
          source_logs :: [Log.ref()],
          replay_after :: Bedrock.version(),
          last_inclusive :: Bedrock.version()
        ) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def recover_from(t, _, _, _, _) when t.mode != :locked, do: {:error, :lock_required, t}

  def recover_from(t, _authority, _source_logs, replay_after, last_inclusive) when replay_after > last_inclusive,
    do: {:error, :invalid_version_range, t}

  def recover_from(t, authority, source_logs, replay_after, last_inclusive) do
    {:ok, authority} = RecoveryAuthority.new(authority)
    t = prepare_replay(t, replay_after)

    result =
      if replay_after == last_inclusive do
        persist_empty_baseline(t, replay_after)
      else
        pull_transactions_from_sources(t, authority, source_logs, replay_after, last_inclusive)
      end

    case result do
      {:ok, %{last_version: ^last_inclusive} = t} -> {:ok, %{t | mode: :locked}}
      {:ok, t} -> {:error, {:incomplete_replay, t.last_version, last_inclusive}, lock_failed_replay(t)}
      {:error, reason, t} -> {:error, reason, lock_failed_replay(t)}
    end
  end

  @doc false
  @spec pull_transactions(State.t(), Log.ref(), Bedrock.version(), Bedrock.version()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def pull_transactions(%{recovery_authority: nil} = t, _log_ref, _replay_after, _last_inclusive),
    do: {:error, :invalid_recovery_authority, t}

  def pull_transactions(t, log_ref, replay_after, last_inclusive),
    do: pull_transactions(t, t.recovery_authority, log_ref, replay_after, last_inclusive)

  @spec recover_transactions(State.t(), [Transaction.encoded()], Bedrock.version(), Bedrock.version()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def recover_transactions(t, transactions, replay_after, last_inclusive) do
    t = prepare_replay(t, replay_after)

    result =
      transactions
      |> Enum.reduce_while({replay_after, t}, fn bytes, acc ->
        process_transaction_bytes(bytes, acc, last_inclusive)
      end)
      |> case do
        {:error, reason, t} -> {:error, reason, t}
        {^last_inclusive, t} -> {:ok, t}
        {cursor, t} -> {:error, {:incomplete_replay, cursor, last_inclusive}, t}
      end

    case result do
      {:ok, t} -> {:ok, %{t | mode: :locked}}
      {:error, reason, t} -> {:error, reason, lock_failed_replay(t)}
    end
  end

  @spec fetch_transactions_from_sources(RecoveryAuthority.input(), [Log.ref()], Bedrock.version(), Bedrock.version()) ::
          {:ok, [Transaction.encoded()]} | {:error, term()}
  def fetch_transactions_from_sources(authority, sources, replay_after, last_inclusive) do
    with {:ok, authority} <- RecoveryAuthority.new(authority) do
      Enum.reduce_while(sources, {:error, :no_source_logs_available}, fn source, _last_error ->
        case fetch_source(authority, source, replay_after, last_inclusive, []) do
          {:ok, transactions} -> {:halt, {:ok, transactions}}
          {:error, _} = error -> {:cont, error}
        end
      end)
    end
  end

  @spec stream_transactions_from_sources(
          pid(),
          reference(),
          RecoveryAuthority.input(),
          [Log.ref()],
          Bedrock.version(),
          Bedrock.version()
        ) :: :ok | {:error, term()}
  def stream_transactions_from_sources(server, operation_id, authority, sources, replay_after, last_inclusive) do
    with {:ok, authority} <- RecoveryAuthority.new(authority) do
      stream_sources(server, operation_id, authority, sources, replay_after, last_inclusive)
    end
  end

  defp stream_sources(_server, _id, _authority, [], _cursor, _last), do: {:error, :no_source_logs_available}

  defp stream_sources(server, id, authority, [source | rest], cursor, last) do
    case stream_source(server, id, authority, source, cursor, last, false) do
      {:error, _reason, false} -> stream_sources(server, id, authority, rest, cursor, last)
      {:error, reason, true} -> {:error, reason}
      :ok -> :ok
    end
  end

  defp stream_source(_server, _id, _authority, _source, cursor, cursor, _started), do: :ok

  defp stream_source(server, id, authority, source, cursor, last, started) do
    case Log.pull(source, cursor,
           recovery: true,
           recovery_authority: RecoveryAuthority.external(authority),
           last_version: last,
           limit: 100
         ) do
      {:ok, []} ->
        {:error, {:incomplete_replay, cursor, last}, started}

      {:ok, transactions} ->
        with {:ok, next_cursor} <- validate_fetched_page(transactions, cursor, last),
             :ok <- deliver_page(server, id, authority, transactions) do
          stream_source(server, id, authority, source, next_cursor, last, true)
        else
          {:error, reason} -> {:error, reason, started}
        end

      {:error, reason} ->
        {:error, {:log_pull_failed, reason, source}, started}
    end
  end

  defp deliver_page(server, id, authority, transactions) do
    send(server, {:replay_page, id, RecoveryAuthority.external(authority), transactions, self()})

    receive do
      {:replay_page_ack, ^id, :ok} -> :ok
      {:replay_page_ack, ^id, {:error, reason}} -> {:error, reason}
    after
      30_000 -> {:error, :destination_timeout}
    end
  end

  @spec prepare_replay_state(State.t(), Bedrock.version()) :: State.t()
  def prepare_replay_state(t, replay_after), do: prepare_replay(t, replay_after)

  @spec apply_replay_page(State.t(), [Transaction.encoded()], Bedrock.version()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def apply_replay_page(t, transactions, last_inclusive) do
    transactions
    |> Enum.reduce_while({t.last_version, t}, fn bytes, acc ->
      process_transaction_bytes(bytes, acc, last_inclusive)
    end)
    |> case do
      {:error, reason, t} -> {:error, reason, t}
      {_cursor, t} -> {:ok, t}
    end
  end

  @spec replay_complete_on_disk?(Path.t(), Bedrock.version(), Bedrock.version()) :: boolean()
  def replay_complete_on_disk?(path, replay_after, last_inclusive) do
    with {:ok, segments} <- ColdStarting.reload_segments_at_path(path),
         true <- segments != [],
         chronological = Enum.reverse(segments),
         true <- hd(chronological).previous_version == replay_after,
         transactions =
           Enum.flat_map(chronological, fn segment ->
             segment |> Segment.ensure_transactions_are_loaded() |> Segment.transactions() |> Enum.reverse()
           end),
         {:ok, cursor} <- validate_fetched_page(transactions, replay_after, last_inclusive) do
      cursor == last_inclusive and valid_segment_chain?(chronological)
    else
      _ -> false
    end
  end

  defp valid_segment_chain?([_]), do: true

  defp valid_segment_chain?(segments) do
    segments
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.all?(fn [previous, next] ->
      previous = Segment.ensure_transactions_are_loaded(previous)
      (Segment.last_version(previous) || previous.previous_version) == next.previous_version
    end)
  end

  defp fetch_source(_authority, _source, cursor, cursor, acc), do: {:ok, Enum.reverse(acc)}

  defp fetch_source(authority, source, cursor, last_inclusive, acc) do
    case Log.pull(source, cursor,
           recovery: true,
           recovery_authority: RecoveryAuthority.external(authority),
           last_version: last_inclusive
         ) do
      {:ok, []} ->
        {:error, {:incomplete_replay, cursor, last_inclusive}}

      {:ok, transactions} ->
        with {:ok, next_cursor} <- validate_fetched_page(transactions, cursor, last_inclusive) do
          fetch_source(authority, source, next_cursor, last_inclusive, Enum.reverse(transactions, acc))
        end

      {:error, :unavailable} ->
        {:error, {:source_log_unavailable, source}}

      {:error, reason} ->
        {:error, {:log_pull_failed, reason, source}}
    end
  end

  defp validate_fetched_page(transactions, cursor, last_inclusive) do
    Enum.reduce_while(transactions, {:ok, cursor}, fn bytes, {:ok, previous} ->
      case Transaction.commit_version(bytes) do
        {:ok, version} when is_binary(version) and version > previous and version <= last_inclusive ->
          {:cont, {:ok, version}}

        {:ok, version} when is_binary(version) and version > last_inclusive ->
          {:halt, {:error, {:transaction_beyond_replay_endpoint, version, last_inclusive}}}

        {:ok, version} when is_binary(version) ->
          {:halt, {:error, {:transaction_not_after_cursor, version, previous}}}

        {:ok, nil} ->
          {:halt, {:error, :missing_transaction_id}}

        {:error, _} ->
          {:halt, {:error, :invalid_transaction}}
      end
    end)
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

          # No chunk cleanup pass: cut broadcasts are gated on the known-committed
          # version, so chunks can never contain versions a recovery would discard.
          # Deterministic replay re-produces byte-identical chunks, and
          # `:already_exists` is always a truthful confirmation.

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

  @spec pull_transactions_from_sources(
          t :: State.t(),
          authority :: RecoveryAuthority.input(),
          source_logs :: [Log.ref()],
          replay_after :: Bedrock.version(),
          last_inclusive :: Bedrock.version()
        ) ::
          {:ok, State.t()} | {:error, term(), State.t()}

  def pull_transactions_from_sources(t, _authority, [], _replay_after, _last_inclusive),
    do: {:error, :no_source_logs_available, t}

  # Single source log - use original behavior
  def pull_transactions_from_sources(t, authority, [source_log], replay_after, last_inclusive) do
    pull_transactions(t, authority, source_log, replay_after, last_inclusive)
  end

  # Multiple source logs - try each in order until one succeeds
  # All logs have the same version sequence, so any survivor works
  def pull_transactions_from_sources(t, authority, source_logs, replay_after, last_inclusive) do
    try_pull_from_sources(t, authority, source_logs, replay_after, last_inclusive, [])
  end

  defp try_pull_from_sources(t, _authority, [], _replay_after, _last_inclusive, errors) do
    # All sources failed, return the last error
    case errors do
      [reason | _] -> {:error, reason, t}
      _ -> {:error, :no_source_logs_available, t}
    end
  end

  defp try_pull_from_sources(t, authority, [source_log | rest], replay_after, last_inclusive, errors) do
    case pull_transactions(t, authority, source_log, replay_after, last_inclusive) do
      {:ok, t} ->
        {:ok, t}

      {:error, {:source_log_unavailable, _} = reason, t} ->
        # A source may disappear between pages. Reset the partial destination
        # before trying another survivor so bytes from attempts cannot mix.
        t = prepare_replay(t, replay_after)
        try_pull_from_sources(t, authority, rest, replay_after, last_inclusive, [reason | errors])

      {:error, _reason, _t} = error ->
        error
    end
  end

  @spec pull_transactions(
          t :: State.t(),
          authority :: RecoveryAuthority.input(),
          log_ref :: Log.ref(),
          replay_after :: Bedrock.version(),
          last_inclusive :: Bedrock.version()
        ) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def pull_transactions(t, _authority, _, replay_after, last_inclusive) when replay_after == last_inclusive,
    do: {:ok, t}

  def pull_transactions(t, authority, log_ref, replay_after, last_inclusive) do
    with_result =
      with {:ok, authority} <- RecoveryAuthority.new(authority) do
        Log.pull(log_ref, replay_after,
          recovery: true,
          recovery_authority: RecoveryAuthority.external(authority),
          last_version: last_inclusive
        )
      end

    case with_result do
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
          {next_replay_after, t} -> pull_transactions(t, authority, log_ref, next_replay_after, last_inclusive)
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
  defp handle_valid_transaction_bytes(bytes, version, _cursor, t) do
    # The source stream is already validated and strictly ordered
    # (process_versioned_transaction), so replay uses the effect-free
    # append primitive directly — no predecessor scheduling, no parking,
    # no acknowledgement plumbing.
    with {:ok, _transaction} <- Transaction.decode(bytes),
         {:ok, t, {^version, _}} <- append_transaction(t, bytes) do
      # Replay routes through the fresh Demux so the replayed range re-enters
      # the chunk pipeline and re-confirms deterministically.
      push_to_demux(t, version, bytes)
      {:cont, {version, t}}
    else
      {:error, :invalid_format} -> {:halt, {:error, :invalid_transaction, t}}
      {:error, reason, t} -> {:halt, {:error, reason, t}}
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

  # Parked pushes hold opaque caller tokens (GenServer froms), not
  # closures; recovery runs inside the server process, so replying to
  # them here is the server replying.
  @spec abort_all_pending_pushes(State.t()) :: State.t()
  def abort_all_pending_pushes(%{pending_pushes: pending_pushes} = t) do
    Enum.each(pending_pushes, fn
      {_version, %{waiters: waiters}} -> Enum.each(waiters, &GenServer.reply(&1, {:error, :not_ready}))
      {_version, {_transaction, from}} -> GenServer.reply(from, {:error, :not_ready})
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
