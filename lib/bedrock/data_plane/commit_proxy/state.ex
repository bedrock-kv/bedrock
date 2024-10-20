defmodule Bedrock.DataPlane.CommitProxy.State do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy.Commit

  @type t :: %__MODULE__{
          cluster: Bedrock.Cluster.t(),
          transaction_system_layout: TransactionSystemLayout.t(),
          epoch: Bedrock.epoch(),
          commit: Commit.t() | nil,
          max_latency_in_ms: non_neg_integer(),
          max_per_batch: non_neg_integer()
        }
  defstruct cluster: nil,
            transaction_system_layout: nil,
            epoch: nil,
            commit: nil,
            max_latency_in_ms: nil,
            max_per_batch: nil
end
