defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicy do
  @moduledoc """
  Decides whether an upload opportunity is worth a snapshot.

  A materializer's snapshot is a compaction output — the bundle a cold
  start restores from is exactly what `Database.compact/4` writes — so
  the moments an upload is possible are the moments a compaction has
  just finished. At that instant the upload is nearly free. Free is not
  the same as wanted: a shard that compacts often would ship a full copy
  of itself every time, and every one of those objects is stored,
  billed, listed, and eventually reclaimed. This policy is the knob that
  says how much of that cadence is worth paying for.

  Two kinds of knob, composed as a floor AND a trigger:

    * `min_interval_ms` is a FLOOR — never upload again within this many
      milliseconds of the last one. Unset, there is no floor.
    * `after_bytes` and `after_transactions` are TRIGGERS on the work
      applied since the last upload — upload only once enough has
      accumulated. They are a disjunction with each other; unset, every
      opportunity qualifies.

  With nothing set both halves are vacuously true and the policy uploads
  at every opportunity, which is the behaviour a materializer has when
  nothing is configured.

  This is a throttle, not a scheduler. Nothing here makes an upload
  HAPPEN; it only declines ones that would have. A running materializer
  currently gets an opportunity when something asks it to compact and
  when it spins down, and nothing in the tree drives the former on a
  cadence — a scheduled snapshot needs a mechanism that produces the
  bundle without a full compaction cutover, which is its own problem
  (bedrock-zi44 notes).

  The counters advance on dispatch, not on completion: the compaction
  upload is fire-and-forget, so the policy paces ATTEMPTS. A failed
  upload is logged and picked up at the next opportunity that clears the
  floor, which is no sooner than `min_interval_ms` away.

  This policy is only about how often a snapshot is WRITTEN. Which of
  the written snapshots survive is retention, and that lives on the
  ObjectStorage side (bedrock-s1zr).
  """

  @type t :: %__MODULE__{
          min_interval_ms: pos_integer() | nil,
          after_bytes: pos_integer() | nil,
          after_transactions: pos_integer() | nil,
          last_upload_at: integer() | nil,
          bytes_since_upload: non_neg_integer(),
          transactions_since_upload: non_neg_integer()
        }
  defstruct min_interval_ms: nil,
            after_bytes: nil,
            after_transactions: nil,
            last_upload_at: nil,
            bytes_since_upload: 0,
            transactions_since_upload: 0

  @doc """
  Builds a policy from a worker's manifest params.

  Same discipline as `idle_timeout`: a knob is set only by an explicit
  positive integer, so a missing or malformed param leaves it off rather
  than turning into an accidental cadence.
  """
  @spec from_params(%{optional(String.t()) => term()}) :: t()
  def from_params(params) do
    %__MODULE__{
      min_interval_ms: knob(params["snapshot_min_interval_ms"]),
      after_bytes: knob(params["snapshot_after_bytes"]),
      after_transactions: knob(params["snapshot_after_transactions"])
    }
  end

  @spec knob(term()) :: pos_integer() | nil
  defp knob(value) when is_integer(value) and value > 0, do: value
  defp knob(_value), do: nil

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
  `:upload` when this opportunity clears the floor and meets a
  threshold, `:wait` otherwise.

  `now_in_ms` is a monotonic reading supplied by the caller, which keeps
  the decision a pure function of the policy and the clock.
  """
  @spec decide(t(), now_in_ms :: integer()) :: :upload | :wait
  def decide(%__MODULE__{} = t, now_in_ms) do
    if floor_cleared?(t, now_in_ms) and thresholds_met?(t), do: :upload, else: :wait
  end

  # Monotonic time is per-VM, so a restart has no last upload to be too
  # close to and the floor is clear. Erring toward a snapshot is the
  # cheap direction: the object key is the durable version, and
  # `Snapshot.write/3` is put-if-not-exists.
  @spec floor_cleared?(t(), integer()) :: boolean()
  defp floor_cleared?(%__MODULE__{min_interval_ms: nil}, _now_in_ms), do: true
  defp floor_cleared?(%__MODULE__{last_upload_at: nil}, _now_in_ms), do: true
  defp floor_cleared?(%__MODULE__{} = t, now_in_ms), do: now_in_ms - t.last_upload_at >= t.min_interval_ms

  @spec thresholds_met?(t()) :: boolean()
  defp thresholds_met?(%__MODULE__{after_bytes: nil, after_transactions: nil}), do: true

  defp thresholds_met?(%__MODULE__{} = t),
    do: reached?(t.after_bytes, t.bytes_since_upload) or reached?(t.after_transactions, t.transactions_since_upload)

  @spec reached?(pos_integer() | nil, non_neg_integer()) :: boolean()
  defp reached?(nil, _accumulated), do: false
  defp reached?(threshold, accumulated), do: accumulated >= threshold

  @doc """
  Restarts the floor and clears the thresholds after an upload has been
  dispatched.
  """
  @spec uploaded(t(), now_in_ms :: integer()) :: t()
  def uploaded(%__MODULE__{} = t, now_in_ms),
    do: %{t | last_upload_at: now_in_ms, bytes_since_upload: 0, transactions_since_upload: 0}
end
