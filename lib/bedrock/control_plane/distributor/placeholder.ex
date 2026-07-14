defmodule Bedrock.ControlPlane.Distributor.Placeholder do
  @moduledoc """
  The placeholder materializer: the cluster's fallback coverage endpoint.

  One placeholder process runs per cluster, started and supervised by the
  Distributor. Its pid fills every uncovered `shard_materializers` slot in
  the transaction system layout, so from a client's perspective every shard
  is always covered: the placeholder speaks the Materializer read API
  (`{:get, key_or_selector, version, opts}` and
  `{:get_range, start, end, version, opts}` GenServer calls) and clients
  cannot distinguish it from a real materializer.

  On each read request the placeholder resolves the shard tag from its own
  copy of the shard layout, then:

    * If a live materializer is already known for the tag (a `{:covered,
      tag, pid}` notification arrived, but stale clients still hold the old
      layout), the request is **forwarded** - re-issued against the live
      materializer from a task, acting as a staleness shim until layout
      updates propagate.
    * Otherwise the request is **parked** in a per-tag waiting list with a
      deadline of `min(caller timeout, hold_ms)` and a `{:coverage_demand,
      tag}` signal is cast to the Distributor (at most one demand per tag
      until the tag is covered or recruitment fails).

  When the Distributor delivers `{:covered, tag, pid}` the tag's parked
  requests are drained by re-issuing each against the new materializer.
  When it delivers `{:coverage_failed, tag, reason}` - or a parked
  request's deadline expires - waiters receive `{:error, :unavailable}`,
  which clients already handle and retry.
  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @default_hold_ms 2_000

  @doc "The default maximum time a request may be parked awaiting coverage."
  @spec default_hold_ms() :: pos_integer()
  def default_hold_ms, do: @default_hold_ms

  @doc """
  Notifies the placeholder that a shard tag is now covered by a live
  materializer. Parked requests for the tag are drained by re-issuing them
  against the materializer, and subsequent requests are forwarded to it.
  """
  @spec notify_covered(ref(), Bedrock.range_tag(), materializer :: pid()) :: :ok
  def notify_covered(placeholder, tag, materializer), do: cast(placeholder, {:covered, tag, materializer})

  @doc """
  Notifies the placeholder that recruitment for a shard tag has failed.
  Parked requests for the tag are shed with `{:error, :unavailable}` and
  the demand dedupe is cleared so a later request re-triggers recruitment.
  """
  @spec notify_coverage_failed(ref(), Bedrock.range_tag(), reason :: term()) :: :ok
  def notify_coverage_failed(placeholder, tag, reason), do: cast(placeholder, {:coverage_failed, tag, reason})
end
