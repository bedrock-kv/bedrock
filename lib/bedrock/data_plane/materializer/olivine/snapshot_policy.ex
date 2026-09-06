defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicy do
  @moduledoc """
  Decides WHEN a materializer uploads a snapshot to ObjectStorage.

  Snapshot uploads are opportunistic: a compaction has just produced the
  bundle-shaped files, so the upload is nearly free at that moment. Free
  is not the same as wanted — a shard that compacts often would ship a
  full copy of itself every time, and every one of those objects is
  billed, listed, and eventually reclaimed. This policy is the knob that
  says how much of that cadence is worth paying for.

  Triggers are a DISJUNCTION and are evaluated at each upload
  opportunity: a scheduled interval since the last upload, bytes applied
  since the last upload, transactions applied since the last upload.
  Whichever fires first wins. A trigger left `nil` never fires; with all
  three `nil` the policy uploads at every opportunity, which is the
  behaviour a materializer has when nothing is configured.

  The counters advance on dispatch, not on completion: the compaction
  upload is fire-and-forget, so the policy paces ATTEMPTS. A failed
  upload is logged and picked up by the next opportunity whose trigger
  fires.

  This policy is only about how often a snapshot is WRITTEN. Which of
  the written snapshots survive is retention, and that lives on the
  ObjectStorage side (bedrock-s1zr).
  """

  @type t :: %__MODULE__{
          interval_ms: pos_integer() | nil,
          after_bytes: pos_integer() | nil,
          after_transactions: pos_integer() | nil,
          last_upload_at: integer() | nil,
          bytes_since_upload: non_neg_integer(),
          transactions_since_upload: non_neg_integer()
        }
  defstruct interval_ms: nil,
            after_bytes: nil,
            after_transactions: nil,
            last_upload_at: nil,
            bytes_since_upload: 0,
            transactions_since_upload: 0

  @doc """
  Builds a policy from a worker's manifest params.

  Same discipline as `idle_timeout`: a trigger is armed only by an
  explicit positive integer, so a missing or malformed param leaves it
  disabled rather than turning into an accidental cadence.
  """
  @spec from_params(%{optional(String.t()) => term()}) :: t()
  def from_params(params) do
    %__MODULE__{
      interval_ms: trigger(params["snapshot_interval_ms"]),
      after_bytes: trigger(params["snapshot_after_bytes"]),
      after_transactions: trigger(params["snapshot_after_transactions"])
    }
  end

  @spec trigger(term()) :: pos_integer() | nil
  defp trigger(value) when is_integer(value) and value > 0, do: value
  defp trigger(_value), do: nil

  @doc """
  Records a batch of applied transactions against the thresholds.
  """
  @spec observe(t(), transactions :: non_neg_integer(), bytes :: non_neg_integer()) :: t()
  def observe(%__MODULE__{} = t, transactions, bytes) do
    %{
      t
      | transactions_since_upload: t.transactions_since_upload + transactions,
        bytes_since_upload: t.bytes_since_upload + bytes
    }
  end

  @doc """
  `:upload` if any configured trigger has fired, `:wait` otherwise.

  `now_in_ms` is a monotonic reading supplied by the caller, which keeps
  the decision a pure function of the policy and the clock.
  """
  @spec decide(t(), now_in_ms :: integer()) :: :upload | :wait
  def decide(%__MODULE__{interval_ms: nil, after_bytes: nil, after_transactions: nil}, _now_in_ms), do: :upload

  def decide(%__MODULE__{} = t, now_in_ms) do
    if interval_elapsed?(t, now_in_ms) or
         reached?(t.after_bytes, t.bytes_since_upload) or
         reached?(t.after_transactions, t.transactions_since_upload) do
      :upload
    else
      :wait
    end
  end

  @spec interval_elapsed?(t(), integer()) :: boolean()
  defp interval_elapsed?(%__MODULE__{interval_ms: nil}, _now_in_ms), do: false
  defp interval_elapsed?(%__MODULE__{last_upload_at: nil}, _now_in_ms), do: true
  defp interval_elapsed?(%__MODULE__{} = t, now_in_ms), do: now_in_ms - t.last_upload_at >= t.interval_ms

  @spec reached?(pos_integer() | nil, non_neg_integer()) :: boolean()
  defp reached?(nil, _accumulated), do: false
  defp reached?(threshold, accumulated), do: accumulated >= threshold

  @doc """
  Restarts the interval and clears the thresholds after an upload has
  been dispatched.
  """
  @spec uploaded(t(), now_in_ms :: integer()) :: t()
  def uploaded(%__MODULE__{} = t, now_in_ms),
    do: %{t | last_upload_at: now_in_ms, bytes_since_upload: 0, transactions_since_upload: 0}
end
