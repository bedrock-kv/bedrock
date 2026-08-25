defmodule Bedrock.DataPlane.CommitProxy.State do
  @moduledoc false

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.RoutingData

  @type mode :: :locked | :running

  @type t :: %__MODULE__{
          cluster: module(),
          director: pid(),
          sequencer: pid() | nil,
          resolver_layout: ResolverLayout.t() | nil,
          epoch: Bedrock.epoch(),
          batch: Batch.t() | nil,
          max_latency_in_ms: non_neg_integer(),
          max_per_batch: non_neg_integer(),
          recent_batch_fill: float(),
          empty_transaction_timeout_ms: non_neg_integer(),
          mode: mode(),
          lock_token: binary(),
          routing_data: RoutingData.t() | nil,
          applied_version: Bedrock.version() | nil,
          batch_seq: non_neg_integer(),
          routed_seq: non_neg_integer(),
          pending_applies: %{pos_integer() => {GenServer.from(), Bedrock.version(), term()}}
        }
  defstruct cluster: nil,
            director: nil,
            sequencer: nil,
            resolver_layout: nil,
            epoch: nil,
            batch: nil,
            max_latency_in_ms: nil,
            max_per_batch: nil,
            # Moving average of how full recent batches were; decides
            # whether holding an open batch would collect anything.
            recent_batch_fill: 1.0,
            empty_transaction_timeout_ms: nil,
            mode: :locked,
            lock_token: nil,
            routing_data: nil,
            # The highest metadata-window to_version this proxy has applied.
            # Its one reader is the tiling assert: every window's from must
            # equal it, or the proxy exits into recovery.
            applied_version: nil,
            # Proxy-local batch sequence, assigned when a batch's finalization
            # is spawned (batches are created and spawned one at a time in the
            # server, so sequence order IS commit-version order). Apply
            # requests are served strictly in this order - FDB's
            # latestLocalCommitBatchLogging, a per-proxy counter, deliberately
            # NOT the global sequencer versions, which interleave across
            # proxies.
            batch_seq: 0,
            # The sequence of the last batch whose metadata was applied and
            # routing snapshot handed out.
            routed_seq: 0,
            # Apply requests that arrived ahead of their predecessor, keyed by
            # their own sequence number.
            pending_applies: %{}
end
