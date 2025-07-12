defmodule Bedrock.DataPlane.CommitProxy.State do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy.Batch

  @type t :: %__MODULE__{
          cluster: module(),
          director: pid(),
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          epoch: Bedrock.epoch(),
          batch: Batch.t() | nil,
          max_latency_in_ms: non_neg_integer(),
          max_per_batch: non_neg_integer()
        }
  defstruct cluster: nil,
            director: nil,
            transaction_system_layout: nil,
            epoch: nil,
            batch: nil,
            max_latency_in_ms: nil,
            max_per_batch: nil
end
