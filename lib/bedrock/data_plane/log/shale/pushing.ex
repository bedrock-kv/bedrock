defmodule Bedrock.DataPlane.Log.Shale.Pushing do
  @moduledoc false
  import Bedrock.DataPlane.Log.Telemetry

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @type appended_transactions :: [Transaction.encoded()]
  @type append_error :: {:error, term(), State.t(), appended_transactions()}
  @type wal_limit_error ::
          {:recovery_required,
           {:wal_limit_exceeded,
            %{
              commit_version: Bedrock.version(),
              min_durable_version: Bedrock.version(),
              last_version: Bedrock.version(),
              lag_us: pos_integer(),
              limit_us: non_neg_integer()
            }}}

  @spec push(
          t :: State.t(),
          expected_version :: Bedrock.version(),
          encoded_transaction :: Transaction.encoded(),
          ack_fn :: (:ok | {:error, term()} -> :ok)
        ) ::
          {:ok, State.t(), appended_transactions()}
          | {:wait, State.t()}
          | append_error()
          | {:error, :not_ready | :tx_out_of_order | :tx_too_large}
  def push(%{mode: :locked}, _, _, _) do
    {:error, :not_ready}
  end

  def push(_, _, encoded_transaction, _ack_fn) when byte_size(encoded_transaction) > 10_000_000 do
    {:error, :tx_too_large}
  end

  def push(t, expected_version, encoded_transaction, ack_fn) when expected_version == t.last_version do
    case write_encoded_transaction(t, encoded_transaction) do
      {:ok, t} ->
        trace_push_transaction(encoded_transaction)
        :ok = ack_fn.(:ok)
        do_pending_pushes(t, [encoded_transaction])

      {:error, {:recovery_required, {:wal_limit_exceeded, _}} = reason, t} ->
        # Version assignment and resolution have already scheduled this
        # link for commit. Refusing its WAL append invalidates the epoch;
        # release every successor so the commit proxies can fail fast and
        # the Director can recover instead of leaving callers stranded.
        :ok = ack_fn.({:error, reason})
        {:error, reason, reject_pending_pushes(t, reason), []}

      {:error, reason, t} ->
        :ok = ack_fn.({:error, reason})
        {:error, reason, t, []}
    end
  end

  def push(t, expected_version, encoded_transaction, ack_fn) when expected_version > t.last_version do
    case admit(t, commit_version_or_nil(encoded_transaction)) do
      :ok ->
        {:wait, Map.update!(t, :pending_pushes, &Map.put(&1, expected_version, {encoded_transaction, ack_fn}))}

      {:error, _reason} = error ->
        error
    end
  end

  def push(t, expected_version, _, _) do
    trace_push_out_of_order(expected_version, t.last_version)
    {:error, :tx_out_of_order}
  end

  @spec do_pending_pushes(State.t()) ::
          {:ok, State.t(), appended_transactions()} | append_error()
  def do_pending_pushes(t), do: do_pending_pushes(t, [])

  defp do_pending_pushes(t, appended) do
    next_expected_version = t.last_version

    case Map.pop(t.pending_pushes, next_expected_version) do
      {nil, _} ->
        {:ok, t, Enum.reverse(appended)}

      {{encoded_transaction, ack_fn}, pending_pushes} ->
        t_with_updated_pending = %{t | pending_pushes: pending_pushes}

        case write_encoded_transaction(t_with_updated_pending, encoded_transaction) do
          {:ok, new_t} ->
            trace_push_transaction(encoded_transaction)
            :ok = ack_fn.(:ok)
            do_pending_pushes(new_t, [encoded_transaction | appended])

          {:error, {:recovery_required, {:wal_limit_exceeded, _}} = reason, error_t} ->
            # The admitted prefix remains the durable tip. The failed link
            # makes every queued successor unusable in this epoch, so wake
            # all of their callers and let coordinated recovery decide the
            # committed prefix.
            :ok = ack_fn.({:error, reason})
            {:error, reason, reject_pending_pushes(error_t, reason), Enum.reverse(appended)}

          {:error, reason, error_t} ->
            :ok = ack_fn.({:error, reason})
            {:error, reason, error_t, Enum.reverse(appended)}
        end
    end
  end

  @spec write_encoded_transaction(State.t(), Transaction.encoded()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def write_encoded_transaction(t, encoded_transaction) when is_nil(t.writer) do
    version =
      case Transaction.commit_version(encoded_transaction) do
        {:ok, version} ->
          version

        {:error, reason} ->
          raise "Failed to extract version: #{inspect(reason)}"
      end

    previous_version = t.last_version

    with :ok <- admit(t, version),
         {:ok, new_segment} <-
           Segment.allocate_from_recycler(t.segment_recycler, t.path, version, previous_version) do
      stage_successor_and_append(t, new_segment, encoded_transaction, version)
    else
      {:error, reason} -> {:error, reason, t}
    end
  end

  def write_encoded_transaction(t, encoded_transaction) do
    with {:ok, version} <- Transaction.commit_version(encoded_transaction),
         :ok <- admit(t, version) do
      maybe_roll_and_append(t, encoded_transaction, version)
    else
      {:error, reason} -> {:error, reason, t}
    end
  end

  defp maybe_roll_and_append(t, encoded_transaction, version) do
    if crosses_cut_boundary?(t.active_segment, version) do
      # Roll on the cut cadence, not just on byte size: the active
      # segment is trim-immune, so a low-traffic log that never fills a
      # segment would otherwise hold its entire history in one
      # untrimmable file — and every recovery would copy that history
      # from version zero. Rolling per cut bucket gives trimming a
      # successor header whose persisted predecessor keeps
      # `available_after` chasing the untrimmed tail. A roll is a rename
      # from the preallocated pool.
      case Writer.close(t.writer) do
        :ok -> write_encoded_transaction(%{t | writer: nil}, encoded_transaction)
        {:error, reason} -> {:error, reason, t}
      end
    else
      append_encoded_transaction(t, encoded_transaction, version)
    end
  end

  # The successor exists only in local variables until its header and
  # first entry have survived one successful sync — Writer.append is that
  # barrier for both. Only then does ownership transfer: successor becomes
  # the active segment and the predecessor moves to the trim-eligible
  # list, as a single state transition. On any failure the staged
  # successor is closed and recycled, and the caller's state comes back
  # untouched: the predecessor stays active (trim-immune, still owning
  # the durable previous_version boundary) with writer: nil, ready for a
  # later retry. Without the staging, a durability watermark arriving
  # after a failed roll could recycle the predecessor against a successor
  # cursor that was never made durable.
  @spec stage_successor_and_append(State.t(), Segment.t(), Transaction.encoded(), Bedrock.version()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  defp stage_successor_and_append(t, new_segment, encoded_transaction, version) do
    with {:ok, new_writer} <- Writer.open(new_segment.path, new_segment.previous_version, t.writer_opts),
         {:ok, new_writer} <- first_append_or_close(new_writer, encoded_transaction, version) do
      {:ok,
       %{
         t
         | writer: new_writer,
           last_version: version,
           oldest_version: oldest_after_append(t, version),
           active_segment: update_segment_transaction_cache(new_segment, encoded_transaction),
           segments: if(t.active_segment, do: [t.active_segment | t.segments], else: t.segments)
       }}
    else
      {:error, reason} ->
        :ok = Segment.return_to_recycler(new_segment, t.segment_recycler)
        {:error, reason, t}
    end
  end

  # The staged writer is closed at the point of its failure, so the shared
  # failure path above only has to recycle the staged segment.
  defp first_append_or_close(writer, encoded_transaction, version) do
    case Writer.append(writer, encoded_transaction, version) do
      {:ok, writer} ->
        {:ok, writer}

      {:error, reason} ->
        _ = Writer.close(writer)
        {:error, reason}
    end
  end

  # Same version arithmetic as the Demux's deterministic cuts: a segment
  # holds exactly one cut bucket of versions.
  defp crosses_cut_boundary?(nil, _version), do: false

  defp crosses_cut_boundary?(%{min_version: min_version}, version) do
    interval = Demux.Server.default_cut_interval_us()

    div(Version.to_integer(version), interval) > div(Version.to_integer(min_version), interval)
  end

  defp append_encoded_transaction(t, encoded_transaction, version) do
    case Writer.append(t.writer, encoded_transaction, version) do
      {:ok, writer} ->
        # Update the active segment's transaction cache to keep it coherent with disk
        updated_active_segment = update_segment_transaction_cache(t.active_segment, encoded_transaction)

        {:ok,
         %{
           t
           | writer: writer,
             last_version: version,
             oldest_version: oldest_after_append(t, version),
             active_segment: updated_active_segment
         }}

      {:error, :segment_full} ->
        case Writer.close(t.writer) do
          :ok -> write_encoded_transaction(%{t | writer: nil}, encoded_transaction)
          {:error, reason} -> {:error, reason, t}
        end

      {:error, reason} ->
        {:error, reason, t}
    end
  end

  # Whether the retained WAL holds any transaction is a fact the cursors
  # already state: `available_after` is the persisted exclusive floor, and
  # the tip sits exactly on it (`last_version == available_after`) exactly
  # when nothing is retained. Reading segment contents to answer this —
  # the old way — cost a synchronous 64 MiB file read on the first append
  # to every fresh or rolled segment.
  defp oldest_after_append(t, appended_version) do
    if t.last_version == t.available_after, do: appended_version, else: t.oldest_version
  end

  # This optional limit is an epoch-fatal WAL safety fuse, not ordinary
  # retryable backpressure. Once the sequencer has assigned a version and
  # resolvers have processed it, refusing a required WAL append means the
  # current epoch cannot continue: replicas may hold different speculative
  # tails and the sequencer's successor chain includes the refused link.
  #
  # Enforce it against each transaction's own prospective commit version,
  # for direct appends and entries drained from the pending queue. Recovery
  # replay copies already-committed history and must never trip the fuse.
  # A nil limit retains the unbounded posture; without a confirmed floor
  # there is not yet a safe distance to measure.
  @spec admit(State.t(), Bedrock.version() | nil) :: :ok | {:error, wal_limit_error()}
  defp admit(%{mode: :running, reject_pushes_above_lag_us: limit, min_durable_version: floor} = t, version)
       when not is_nil(limit) and not is_nil(floor) and not is_nil(version) do
    lag_us = Version.distance(version, floor)

    if lag_us > limit do
      trace_wal_limit_exceeded(
        floor,
        t.last_version,
        version,
        lag_us,
        limit,
        map_size(t.pending_pushes)
      )

      {:error,
       {:recovery_required,
        {:wal_limit_exceeded,
         %{
           commit_version: version,
           min_durable_version: floor,
           last_version: t.last_version,
           lag_us: lag_us,
           limit_us: limit
         }}}}
    else
      :ok
    end
  end

  defp admit(_t, _version), do: :ok

  defp commit_version_or_nil(encoded_transaction) do
    case Transaction.commit_version(encoded_transaction) do
      {:ok, version} -> version
      {:error, _} -> nil
    end
  end

  defp reject_pending_pushes(%{pending_pushes: pending} = t, reason) do
    Enum.each(pending, fn {_expected, {_transaction, ack_fn}} -> :ok = ack_fn.({:error, reason}) end)
    %{t | pending_pushes: %{}}
  end

  @spec update_segment_transaction_cache(Segment.t(), Transaction.encoded()) :: Segment.t()
  defp update_segment_transaction_cache(segment, encoded_transaction) do
    case segment.transactions do
      nil ->
        # If transactions not loaded, initialize with the new transaction
        %{segment | transactions: [encoded_transaction]}

      existing_transactions ->
        # Prepend new transaction to maintain newest-first order
        %{segment | transactions: [encoded_transaction | existing_transactions]}
    end
  end
end
