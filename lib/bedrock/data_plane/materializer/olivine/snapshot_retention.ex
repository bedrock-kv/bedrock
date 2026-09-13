defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotRetention do
  @moduledoc """
  Decides which of a shard's written snapshots are still worth keeping.

  `SnapshotPolicy` decides how often a snapshot is WRITTEN. This decides
  which of the written ones SURVIVE, and it is the other half of the same
  cost: an object that is never pruned is stored, billed and listed
  forever.

  A snapshot is derived state, not history. It is a compaction output —
  the coalesced contents of the shard at one version — so deleting one
  loses nothing that cannot be rebuilt by replaying the chunks from an
  older snapshot forward. That is what separates this from the chunks
  themselves, which ARE the history and are never deleted (bedrock-wxf.6;
  reclaiming them needs a replay floor, bedrock-wxf.6.11). The only
  invariant retention owes anyone is that a shard never ends up with zero
  snapshots: a cold start with no baseline has to replay the whole shard.

  One knob, `keep_last` — the number of newest snapshots to keep. Unset,
  nothing is ever deleted and the shard behaves exactly as it did before
  retention existed.

  ## Why there is no "keep anything newer than N minutes"

  Because nothing here can honestly answer it. `ObjectStorage.list/3`
  yields keys and nothing else — no modification times — and a snapshot's
  key carries its VERSION, which is a Lamport clock: the sequencer
  advances it by elapsed microseconds *while a cluster is up*
  (`Sequencer.Server.handle_call({:next_commit_version, _}, ...)`) and not
  at all across a restart or an outage. A knob named for wall-clock time
  would silently mean cluster uptime, and would keep or drop a different
  amount after every incident. Counting objects is a question the listing
  can actually answer.
  """

  @type version :: non_neg_integer()

  @type t :: %__MODULE__{
          keep_last: pos_integer() | nil
        }
  defstruct keep_last: nil

  @doc """
  Builds a retention policy from a worker's manifest params.

  Same discipline as `SnapshotPolicy.from_params/1`: a knob is set only by
  an explicit positive integer, so a missing or malformed param leaves
  retention off rather than turning into an accidental deletion.
  """
  @spec from_params(%{optional(String.t()) => term()}) :: t()
  def from_params(params) do
    %__MODULE__{keep_last: knob(params["snapshot_keep_last"])}
  end

  @spec knob(term()) :: pos_integer() | nil
  defp knob(value) when is_integer(value) and value > 0, do: value
  defp knob(_value), do: nil

  @doc """
  Whether this policy would ever delete anything.

  Callers ask before listing: an unconfigured policy must not cost a
  shard so much as one extra request.
  """
  @spec configured?(t()) :: boolean()
  def configured?(%__MODULE__{keep_last: nil}), do: false
  def configured?(%__MODULE__{}), do: true

  @doc """
  The oldest version this policy still wants, given the shard's snapshot
  versions in the newest-first order `Snapshot.list/2` yields them.

  Returns `{:ok, version}` — everything strictly below it may go — or
  `:keep_all` when the policy is unset, or the shard holds no more
  snapshots than the policy keeps and there is nothing below the floor to
  delete anyway.

  The floor is always an EXISTING version from the list — the Kth newest
  — so the newest snapshot can never be the one deleted: with
  `keep_last: 1` the floor is the newest version itself, and nothing is
  strictly below it.
  """
  @spec oldest_to_keep(t(), [version()]) :: {:ok, version()} | :keep_all
  def oldest_to_keep(%__MODULE__{keep_last: nil}, _newest_first), do: :keep_all

  def oldest_to_keep(%__MODULE__{keep_last: keep_last}, newest_first) do
    case Enum.drop(newest_first, keep_last - 1) do
      [oldest_kept, _at_least_one_older | _rest] -> {:ok, oldest_kept}
      _nothing_older -> :keep_all
    end
  end
end
