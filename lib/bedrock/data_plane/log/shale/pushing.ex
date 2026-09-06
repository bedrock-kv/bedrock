defmodule Bedrock.DataPlane.Log.Shale.Pushing do
  @moduledoc """
  Predecessor scheduling and WAL appends, expressed as one uniform
  transition.

  This module owns WAL and resource state only. It never replies to a
  caller, never forwards to the Demux, and never notifies a puller — every
  `push/4` returns the same transition shape, and `Shale.Server` interprets
  it exactly once:

    * `state` — the resulting WAL state.
    * `appended` — the append events, in predecessor-chain order. Each is
      `{version, encoded_transaction}` (the exact bytes written), which is
      everything Demux delivery and puller notification need.
    * `replies` — ordered `{token, result}` instructions. Tokens are the
      opaque values callers were parked or presented with; this module never
      looks inside them and never invokes anything.
    * `parked?` — whether the current request was stored to wait for its
      predecessor (no reply owed yet).

  The tuple-arity protocol this replaces encoded *whether replies had
  already been sent* in the shape of the return value; the server had to
  know which shapes had which side effects. Now the effects are data.
  """
  import Bedrock.DataPlane.Log.Telemetry

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.RecoveryAuthority

  @type append_event :: {Bedrock.version(), Transaction.encoded()}
  @type reply_token :: term()
  @type reply_instruction :: {reply_token(), :ok | {:error, term()}}
  @type transition :: %{
          state: State.t(),
          appended: [append_event()],
          replies: [reply_instruction()],
          parked?: boolean()
        }
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

  @doc """
  Schedules one live push and returns the resulting transition.

  Covers immediate success, parking behind a missing predecessor, rejection
  (locked, oversized, stale, backpressured), ordered multi-entry drains of
  the pending queue, and partial-drain failure — all in the same shape.
  """
  @spec push(State.t(), Bedrock.version(), Transaction.encoded(), reply_token()) :: transition()
  def push(%{recovery_authority: nil} = t, _expected_version, _encoded_transaction, token),
    do: rejection(t, token, :invalid_recovery_authority)

  def push(t, expected_version, encoded_transaction, token),
    do: push(t, t.recovery_authority, expected_version, encoded_transaction, token)

  @spec push(
          State.t(),
          RecoveryAuthority.input(),
          expected_version :: Bedrock.version(),
          Transaction.encoded(),
          reply_token()
        ) ::
          transition()
  def push(t, authority, expected_version, encoded_transaction, token) do
    with {:ok, authority} <- RecoveryAuthority.new(authority),
         :ok <- owns?(t, authority) do
      do_push(t, RecoveryAuthority.external(authority), expected_version, encoded_transaction, token)
    else
      {:error, reason} -> rejection(t, token, reason)
    end
  end

  defp do_push(%{mode: :locked} = t, _, _, _, token), do: rejection(t, token, :not_ready)

  defp do_push(t, _, _, encoded_transaction, token) when byte_size(encoded_transaction) > 10_000_000,
    do: rejection(t, token, :tx_too_large)

  defp do_push(t, _authority, expected_version, encoded_transaction, token) when expected_version == t.last_version do
    case append_transaction(t, encoded_transaction) do
      {:ok, t, event} ->
        trace_push_transaction(encoded_transaction)
        drain(t, [event], [{token, :ok}])

      {:error, {:recovery_required, {:wal_limit_exceeded, _}} = reason, t} ->
        # Version assignment and resolution have already scheduled this
        # link for commit. Refusing its WAL append invalidates the epoch;
        # release every successor so the commit proxies can fail fast and
        # the Director can recover instead of leaving callers stranded.
        {t, flushed} = reject_all_pending(t, reason)
        %{state: t, appended: [], replies: [{token, {:error, reason}} | flushed], parked?: false}

      {:error, reason, t} ->
        rejection(t, token, reason)
    end
  end

  defp do_push(t, authority, expected_version, encoded_transaction, token) when expected_version > t.last_version do
    case admit(t, commit_version_or_nil(encoded_transaction)) do
      :ok ->
        park(t, authority, expected_version, encoded_transaction, token)

      {:error, reason} ->
        rejection(t, token, reason)
    end
  end

  defp do_push(t, _authority, expected_version, encoded_transaction, token) when expected_version < t.last_version do
    if exact_tail_retry?(t, expected_version, encoded_transaction) do
      rejection_result(t, token, :ok)
    else
      reason =
        if commit_version_or_nil(encoded_transaction) == t.last_version,
          do: :conflicting_durable_tail,
          else: :tx_out_of_order

      rejection(t, token, reason)
    end
  end

  defp do_push(t, _authority, expected_version, _, token) do
    trace_push_out_of_order(expected_version, t.last_version)
    rejection(t, token, :tx_out_of_order)
  end

  defp owns?(%{recovery_authority: nil}, _), do: {:error, :not_lock_owner}

  defp owns?(%{recovery_authority: current}, incoming) do
    if RecoveryAuthority.compare(incoming, current) == :same, do: :ok, else: {:error, :not_lock_owner}
  end

  defp park(t, authority, expected_version, transaction, token) do
    case Map.get(t.pending_pushes, expected_version) do
      nil ->
        entry = %{authority: authority, transaction: transaction, waiters: [token]}

        %{
          state: %{t | pending_pushes: Map.put(t.pending_pushes, expected_version, entry)},
          appended: [],
          replies: [],
          parked?: true
        }

      %{authority: ^authority, transaction: ^transaction, waiters: waiters} = entry ->
        entry = %{entry | waiters: waiters ++ [token]}

        %{
          state: %{t | pending_pushes: Map.put(t.pending_pushes, expected_version, entry)},
          appended: [],
          replies: [],
          parked?: true
        }

      _ ->
        rejection(t, token, :conflicting_pending_push)
    end
  end

  defp rejection(t, token, reason), do: %{state: t, appended: [], replies: [{token, {:error, reason}}], parked?: false}
  defp rejection_result(t, token, result), do: %{state: t, appended: [], replies: [{token, result}], parked?: false}

  defp exact_tail_retry?(
         %{active_segment: %{transactions: [tail | rest], previous_version: segment_previous}},
         expected,
         bytes
       ) do
    predecessor =
      case rest do
        [previous | _] -> Transaction.commit_version!(previous)
        [] -> segment_previous
      end

    tail == bytes and predecessor == expected
  end

  defp exact_tail_retry?(_, _, _), do: false

  # Drains the pending queue along the predecessor chain. A drained entry
  # that fails to append gets its error reply; on backpressure the rest of
  # the queue (all later versions, equally inadmissible, gap now
  # unfillable) is rejected too, while other errors leave the remaining
  # entries parked — the admitted prefix is the tip either way.
  defp drain(t, appended, replies) do
    case Map.pop(t.pending_pushes, t.last_version) do
      {nil, _} ->
        %{state: t, appended: Enum.reverse(appended), replies: Enum.reverse(replies), parked?: false}

      {%{authority: authority, transaction: encoded_transaction, waiters: waiters}, pending_pushes} ->
        t = %{t | pending_pushes: pending_pushes}

        case owns?(t, authority) do
          {:error, reason} ->
            {t, flushed} = reject_all_pending(t, reason)

            %{
              state: t,
              appended: Enum.reverse(appended),
              replies: Enum.reverse(Enum.map(waiters, &{&1, {:error, reason}}) ++ replies, flushed),
              parked?: false
            }

          :ok ->
            case append_transaction(t, encoded_transaction) do
              {:ok, t, event} ->
                trace_push_transaction(encoded_transaction)
                drain(t, [event | appended], Enum.map(waiters, &{&1, :ok}) ++ replies)

              {:error, {:recovery_required, {:wal_limit_exceeded, _}} = reason, t} ->
                # The admitted prefix remains the durable tip. The failed link
                # makes every queued successor unusable in this epoch, so wake
                # all of their callers and let coordinated recovery decide the
                # committed prefix.
                {t, flushed} = reject_all_pending(t, reason)

                %{
                  state: t,
                  appended: Enum.reverse(appended),
                  replies: Enum.reverse(Enum.map(waiters, &{&1, {:error, reason}}) ++ replies, flushed),
                  parked?: false
                }

              {:error, reason, t} ->
                %{
                  state: t,
                  appended: Enum.reverse(appended),
                  replies: Enum.reverse(Enum.map(waiters, &{&1, {:error, reason}}) ++ replies),
                  parked?: false
                }
            end
        end
    end
  end

  defp reject_all_pending(t, reason) do
    flushed =
      Enum.flat_map(t.pending_pushes, fn {_expected, %{waiters: waiters}} ->
        Enum.map(waiters, &{&1, {:error, reason}})
      end)

    {%{t | pending_pushes: %{}}, flushed}
  end

  @doc """
  The single-entry, effect-free WAL append primitive.

  Appends one encoded transaction — allocating or rolling segments as
  needed — and returns the new state with the append event, or the error
  with the caller's state intact. The WAL acknowledgement boundary is here:
  a returned event means the bytes and their sync completed. No caller
  reply, no Demux cast, no puller notification.

  Recovery replays through this directly: its source stream is already
  validated and strictly ordered, so it needs the append, not the
  scheduler.
  """
  @spec append_transaction(State.t(), Transaction.encoded()) ::
          {:ok, State.t(), append_event()} | {:error, term(), State.t()}
  def append_transaction(t, encoded_transaction) do
    case Transaction.commit_version(encoded_transaction) do
      {:ok, version} ->
        case do_append(t, encoded_transaction, version) do
          {:ok, t} -> {:ok, t, {version, encoded_transaction}}
          {:error, reason, t} -> {:error, reason, t}
        end

      {:error, reason} ->
        {:error, reason, t}
    end
  end

  defp do_append(t, encoded_transaction, version) when is_nil(t.writer) do
    with :ok <- admit(t, version),
         {:ok, new_segment} <-
           Segment.allocate_from_recycler(t.segment_recycler, t.path, version, t.last_version) do
      stage_successor_and_append(t, new_segment, encoded_transaction, version)
    else
      {:error, reason} -> {:error, reason, t}
    end
  end

  defp do_append(t, encoded_transaction, version) do
    case admit(t, version) do
      :ok -> maybe_roll_and_append(t, encoded_transaction, version)
      {:error, reason} -> {:error, reason, t}
    end
  end

  defp maybe_roll_and_append(t, encoded_transaction, version) do
    if crosses_cut_boundary?(t, t.active_segment, version) do
      # Roll on the cut cadence, not just on byte size: the active
      # segment is trim-immune, so a low-traffic log that never fills a
      # segment would otherwise hold its entire history in one
      # untrimmable file — and every recovery would copy that history
      # from version zero. Rolling per cut bucket gives trimming a
      # successor header whose persisted predecessor keeps
      # `available_after` chasing the untrimmed tail. A roll is a rename
      # from the preallocated pool.
      case Writer.close(t.writer) do
        :ok -> do_append(%{t | writer: nil}, encoded_transaction, version)
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
  # holds exactly one cut bucket of versions. The width comes from the
  # log's own state — the same value `DemuxControl` starts the Demux
  # with — so the two boundaries cannot drift apart.
  defp crosses_cut_boundary?(_t, nil, _version), do: false

  defp crosses_cut_boundary?(t, %{min_version: min_version}, version) do
    interval = State.cut_interval_us(t)

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
          :ok -> do_append(%{t | writer: nil}, encoded_transaction, version)
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
