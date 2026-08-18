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

      {:error, reason, t} ->
        :ok = ack_fn.({:error, reason})
        {:error, reason, t, []}
    end
  end

  def push(t, expected_version, encoded_transaction, ack_fn) when expected_version > t.last_version do
    {:wait, Map.update!(t, :pending_pushes, &Map.put(&1, expected_version, {encoded_transaction, ack_fn}))}
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

    case Segment.allocate_from_recycler(t.segment_recycler, t.path, version, previous_version) do
      {:ok, new_segment} ->
        stage_successor_and_append(t, new_segment, encoded_transaction, version)

      {:error, reason} ->
        {:error, reason, t}
    end
  end

  def write_encoded_transaction(t, encoded_transaction) do
    case Transaction.commit_version(encoded_transaction) do
      {:ok, version} ->
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

      {:error, reason} ->
        {:error, reason, t}
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
           oldest_version: if(wal_has_transactions?(t), do: t.oldest_version, else: version),
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
    had_transactions? = wal_has_transactions?(t)

    case Writer.append(t.writer, encoded_transaction, version) do
      {:ok, writer} ->
        # Update the active segment's transaction cache to keep it coherent with disk
        updated_active_segment = update_segment_transaction_cache(t.active_segment, encoded_transaction)
        oldest_version = if had_transactions?, do: t.oldest_version, else: version

        {:ok,
         %{
           t
           | writer: writer,
             last_version: version,
             oldest_version: oldest_version,
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

  defp wal_has_transactions?(t) do
    [t.active_segment | t.segments]
    |> Enum.reject(&is_nil/1)
    |> Enum.any?(fn segment -> Segment.transactions(segment) != [] end)
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
