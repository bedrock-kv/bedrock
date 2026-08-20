defmodule Bedrock.DataPlane.CommitProxy.State do
  @moduledoc false

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Metadata
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
          empty_transaction_timeout_ms: non_neg_integer(),
          mode: mode(),
          lock_token: binary(),
          routing_data: RoutingData.t() | nil,
          metadata: Metadata.t(),
          deferred_metadata: [{Bedrock.version(), [term()]}],
          batch_seq: non_neg_integer(),
          routed_seq: non_neg_integer(),
          pending_applies: %{pos_integer() => {GenServer.from(), Bedrock.version(), term(), term()}}
        }
  defstruct cluster: nil,
            director: nil,
            sequencer: nil,
            resolver_layout: nil,
            epoch: nil,
            batch: nil,
            max_latency_in_ms: nil,
            max_per_batch: nil,
            empty_transaction_timeout_ms: nil,
            mode: :locked,
            lock_token: nil,
            routing_data: nil,
            metadata: %Metadata{},
            # Committed metadata from sharded batches (version-ascending),
            # re-sent as confirmations on every resolver call until the
            # resolvers' windows show it was folded in (ack >= version).
            deferred_metadata: [],
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
