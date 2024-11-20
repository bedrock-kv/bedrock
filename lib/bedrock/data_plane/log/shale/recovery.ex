defmodule Bedrock.DataPlane.Log.Shale.Recovery do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer

  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 4]

  @spec recover_from(
          State.t(),
          Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()} | {:error, reason :: term()}
  def recover_from(t, _, _, _) when t.mode != :locked,
    do: {:error, :lock_required}

  def recover_from(t, source_log, first_version, last_version) do
    %{t | mode: :recovering}
    |> close_writer()
    |> discard_all_segments()
    |> ensure_active_segment(first_version)
    |> open_writer()
    |> push_sentinel(first_version)
    |> pull_transactions(source_log, first_version, last_version)
    |> case do
      {:ok, t} ->
        {:ok, %{t | mode: :running, oldest_version: first_version, last_version: last_version}}

      error ->
        error
    end
  end

  @spec pull_transactions(
          t :: State.t(),
          log_ref :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | Log.pull_errors()
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
  def pull_transactions(t, _, first_version, last_version) when first_version == last_version,
    do: {:ok, t}

  def pull_transactions(t, log_ref, first_version, last_version) do
    case Log.pull(log_ref, first_version, recovery: true, last_version: last_version) do
      {:ok, []} ->
        {:ok, t}

      {:ok, transactions} ->
        transactions
        |> Enum.reduce_while({first_version, t}, fn
          <<version::unsigned-big-64, _::binary>> = bytes, {last_version, t} ->
            with {:ok, transaction} <- EncodedTransaction.validate(bytes),
                 {:ok, t} <- push(t, last_version, transaction, fn _ -> :ok end) do
              {:cont, {version, t}}
            else
              {:wait, _t} -> {:halt, {:error, :tx_out_of_order}}
              {:error, _reason} = error -> {:halt, error}
            end

          _, _ ->
            {:halt, {:error, :invalid_transaction}}
        end)
        |> case do
          {:error, _reason} = error -> error
          {next_first, t} -> pull_transactions(t, log_ref, next_first, last_version)
        end

      {:error, :unavailable} ->
        {:error, {:source_log_unavailable, log_ref}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def close_writer(%{writer: nil} = t), do: t

  def close_writer(%{writer: writer} = t) do
    :ok = Writer.close(writer)
    %{t | writer: nil}
  end

  def discard_all_segments(%{active_segment: nil, segments: segments} = t),
    do: %{t | segments: discard_segments(t.segment_recycler, segments)}

  def discard_all_segments(%{active_segment: active_segment, segments: segments} = t) do
    %{
      t
      | active_segment: nil,
        segments: discard_segments(t.segment_recycler, [active_segment | segments])
    }
  end

  def discard_segments(_segment_recycler, []), do: []

  def discard_segments(segment_recycler, [segment | remaining_segments]) do
    :ok = SegmentRecycler.check_in(segment_recycler, segment.path)
    discard_segments(segment_recycler, remaining_segments)
  end

  def ensure_active_segment(%{active_segment: nil} = t, version) do
    with {:ok, new_segment} <- Segment.allocate_from_recycler(t.segment_recycler, t.path, version) do
      %{t | active_segment: new_segment, last_version: version}
    else
      {:error, _} -> raise "Failed to allocate new segment"
    end
  end

  def ensure_active_segment(t), do: t

  def open_writer(t) do
    with {:ok, new_writer} <- Writer.open(t.active_segment.path) do
      %{t | writer: new_writer}
    else
      {:error, _} -> raise "Failed to open writer"
    end
  end

  def push_sentinel(t, version) do
    with sentinel <- EncodedTransaction.encode({version, %{}}),
         {:ok, t} <- push(t, version, sentinel, fn _ -> :ok end) do
      t
    else
      {:error, _} -> raise "Failed to push sentinel"
    end
  end
end
