defmodule Bedrock.DataPlane.CommitProxy.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator

  @type mode :: :locked | :running

  @type t :: %__MODULE__{
          cluster: module(),
          director: pid(),
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          resolver_layout: ResolverLayout.t() | nil,
          epoch: Bedrock.epoch(),
          batch: Batch.t() | nil,
          max_latency_in_ms: non_neg_integer(),
          max_per_batch: non_neg_integer(),
          empty_transaction_timeout_ms: non_neg_integer(),
          mode: mode(),
          lock_token: binary(),
          metadata: [MetadataAccumulator.entry()]
        }
  defstruct cluster: nil,
            director: nil,
            transaction_system_layout: nil,
            resolver_layout: nil,
            epoch: nil,
            batch: nil,
            max_latency_in_ms: nil,
            max_per_batch: nil,
            empty_transaction_timeout_ms: nil,
            mode: :locked,
            lock_token: nil,
            metadata: []
end
