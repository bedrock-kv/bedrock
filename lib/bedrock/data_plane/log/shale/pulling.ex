defmodule Bedrock.DataPlane.Log.Shale.Pulling do
  @moduledoc false
  import Bedrock.DataPlane.Log.Shale.TransactionStreams

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction

  @spec pull(
          t :: State.t(),
          from_version :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            last_version: Bedrock.version(),
            recovery: boolean()
          ]
        ) ::
          {:ok, State.t(), [Transaction.encoded()]}
          | {:waiting_for, Bedrock.version()}
          | {:error, :not_ready}
          | {:error, :not_locked}
          | {:error, :invalid_last_version}
          | {:error, :version_too_old}
  def pull(t, from_version, opts \\ [])

  def pull(t, from_version, _) when from_version >= t.last_version, do: {:waiting_for, from_version}

  def pull(t, from_version, _) when from_version < t.oldest_version, do: {:error, :version_too_old}

  def pull(t, from_version, opts) do
    with :ok <- check_for_locked_outside_of_recovery(opts[:recovery] || false, t),
         {:ok, last_version} <- check_last_version(opts[:last_version] || t.last_version, from_version),
         {:ok, [active_segment | remaining_segments] = all_segments} <-
           ensure_necessary_segments_are_loaded(
             last_version,
             [t.active_segment | t.segments]
           ),
         {:ok, transaction_stream} <- from_segments(all_segments, from_version) do
      transactions =
        transaction_stream
        |> until_version(last_version)
        |> at_most(determine_pull_limit(opts[:limit], t))
        |> Enum.to_list()

      {:ok, %{t | active_segment: active_segment, segments: remaining_segments}, transactions}
    else
      {:error, :not_found} ->
        # No transactions found after from_version due to version gaps.
        # For recovery operations, return the error directly since recovery is synchronous.
        # For regular pulls, wait for new transactions like we do for future versions.
        if opts[:recovery] do
          {:error, :not_found}
        else
          {:waiting_for, from_version}
        end

      error ->
        error
    end
  end

  @spec ensure_necessary_segments_are_loaded(Bedrock.version() | nil, [Segment.t()]) ::
          {:ok, [Segment.t()]} | {:error, :version_too_old}
  def ensure_necessary_segments_are_loaded(_, []), do: {:error, :version_too_old}

  def ensure_necessary_segments_are_loaded(nil, [segment | remaining_segments]) do
    segment = Segment.ensure_transactions_are_loaded(segment)
    {:ok, [segment | remaining_segments]}
  end

  def ensure_necessary_segments_are_loaded(last_version, [segment | remaining_segments])
      when segment.min_version <= last_version do
    segment = Segment.ensure_transactions_are_loaded(segment)
    {:ok, [segment | remaining_segments]}
  end

  def ensure_necessary_segments_are_loaded(last_version, [segment | remaining_segments]) do
    segment = Segment.ensure_transactions_are_loaded(segment)

    with {:ok, remaining_segments} <-
           ensure_necessary_segments_are_loaded(last_version, remaining_segments) do
      {:ok, [segment | remaining_segments]}
    end
  end

  @spec check_for_locked_outside_of_recovery(boolean(), State.t()) ::
          :ok | {:error, :not_locked} | {:error, :not_ready}
  def check_for_locked_outside_of_recovery(in_recovery, t)
  def check_for_locked_outside_of_recovery(true, %{mode: :locked}), do: :ok
  def check_for_locked_outside_of_recovery(true, _), do: {:error, :not_locked}
  def check_for_locked_outside_of_recovery(false, %{mode: :locked}), do: {:error, :not_ready}
  def check_for_locked_outside_of_recovery(_, _), do: :ok

  @spec check_last_version(
          last_version :: Bedrock.version() | nil,
          from_version :: Bedrock.version()
        ) :: {:ok, Bedrock.version()} | {:error, :invalid_last_version}
  def check_last_version(nil, _), do: {:ok, nil}

  def check_last_version(last_version, from_version) when last_version >= from_version, do: {:ok, last_version}

  def check_last_version(_, _), do: {:error, :invalid_last_version}

  @spec determine_pull_limit(pos_integer() | nil, State.t()) :: pos_integer()
  def determine_pull_limit(nil, t), do: t.params.default_pull_limit
  def determine_pull_limit(limit, t), do: min(limit, t.params.max_pull_limit)
end
