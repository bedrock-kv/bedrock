defmodule Bedrock.DataPlane.Sequencer.State do
  @type t() :: %__MODULE__{
          cluster: module(),
          controller: pid(),
          epoch: Bedrock.epoch(),
          started_at: integer(),
          last_timestamp_at: integer(),
          sequence: non_neg_integer(),
          max_transactions_per_ms: non_neg_integer(),
          last_committed_version: Bedrock.version() | nil
        }
  defstruct cluster: nil,
            controller: nil,
            epoch: 0,
            started_at: 0,
            last_timestamp_at: 0,
            sequence: 0,
            max_transactions_per_ms: 1_000,
            last_committed_version: nil

  def update_last_committed_version(t, version) do
    %{t | last_committed_version: version}
  end
end
