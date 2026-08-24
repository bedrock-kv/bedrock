defmodule Bedrock.ControlPlane.Distributor.Placeholder do
  @moduledoc """
  The coverage placeholder: a process that speaks the materializer read
  API and parks reads for uncovered shard tags, bounded by
  `min(caller timeout, hold_ms)`, shedding `{:error, :unavailable}` —
  which clients already classify as retryable — when coverage does not
  arrive in time.

  An honest note on what parking buys whom: the transaction builder
  passes its fetch timeout (50ms) as the caller timeout, so for repo
  traffic the effective park is ~50ms per attempt and the caller's own
  clock usually expires first — the REPO's retry-with-backoff loop does
  the actual waiting, and this process serves as a crash-absorbing,
  demand-signaling stall (no noproc storms, gaps visible in the
  keyspace, demand deduped per tag). The full `hold_ms` park is
  reachable only by direct materializer callers with generous or absent
  timeouts. Parked entries self-expire; replies to dead callers are
  no-ops.

  Addressing is the settled option 2 (bedrock-q67.21): the placeholder
  IS a materializer ref — an ordinary MEMBER of the shard's set, one
  that parks rather than serves. The distributor publishes
  `materializers/<tag>/<placeholder_worker_id> = distributor_node` for
  uncovered tags, and the placeholder registers under
  `cluster.otp_name_for_worker(placeholder_worker_id)` — so proxies and
  clients need no special case at all; coverage gaps are visible in the
  keyspace like any other assignment. A corollary: a restarted
  placeholder re-registers the same name on the same node, so restarts
  need no republication.

  When the distributor delivers `{:covered, tag, ref}` the tag's parked
  requests drain by re-issue; `{:coverage_failed, tag, reason}` sheds.
  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @default_hold_ms 2_000

  # The stable worker id the keyspace names for uncovered tags. Worker
  # OTP names are deterministic in the id, so the callable ref is
  # derivable everywhere from the committed {worker_id, node} pair.
  # One source of truth: routing reads this convention too, so it lives
  # with the family's semantics rather than behind the control plane.
  @worker_id Bedrock.SystemKeys.placeholder_worker_id()

  @doc "The reserved worker id placeholder entries carry in the keyspace."
  @spec worker_id() :: String.t()
  def worker_id, do: @worker_id

  @doc "The default maximum time a request may be parked awaiting coverage."
  @spec default_hold_ms() :: pos_integer()
  def default_hold_ms, do: @default_hold_ms

  @doc """
  Notifies the placeholder that a shard tag is now covered by a live
  materializer. Parked requests for the tag are drained by re-issuing
  them against the materializer; subsequent requests forward to it.
  """
  @spec notify_covered(ref(), Bedrock.range_tag(), materializer :: ref()) :: :ok
  def notify_covered(placeholder, tag, materializer), do: cast(placeholder, {:covered, tag, materializer})

  @doc """
  Notifies the placeholder that a shard tag's materializer has died and
  the tag is uncovered again: subsequent requests park and re-demand
  coverage instead of forwarding to the dead ref.
  """
  @spec notify_uncovered(ref(), Bedrock.range_tag()) :: :ok
  def notify_uncovered(placeholder, tag), do: cast(placeholder, {:uncovered, tag})

  @doc """
  Notifies the placeholder that recruitment for a shard tag failed:
  parked requests shed `{:error, :unavailable}` and the demand dedupe
  clears so a later request re-triggers recruitment.
  """
  @spec notify_coverage_failed(ref(), Bedrock.range_tag(), reason :: term()) :: :ok
  def notify_coverage_failed(placeholder, tag, reason), do: cast(placeholder, {:coverage_failed, tag, reason})
end
