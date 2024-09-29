defmodule Bedrock.ControlPlane.DataDistributor.Team do
  @moduledoc """
  A team is a collection of log storage pairs that are responsible for a
  specific key range. The team is responsible for ensuring that the data is
  replicated to the correct number of log storage pairs.
  """

  @typedoc """
  A `Team` is a collection of log storage pairs that are responsible for a
  specific key range.

  ## Fields
    - `tag` - A unique identifier for the team.
    - `key_range` - The range of keys that this team is responsible for.
    - `replication_factor` - The number of log storage pairs that the data
      should be replicated to.
    - `log_storage_pairs` - The log storage pairs that are responsible for
  """
  @type t :: %__MODULE__{
          tag: tag(),
          key_range: key_range(),
          replication_factor: replication_factor(),
          log_storage_pairs: [log_storage_pair()]
        }
  defstruct tag: nil, key_range: nil, replication_factor: nil, log_storage_pairs: []

  alias Bedrock.Service.StorageWorker
  alias Bedrock.Service.TransactionLog

  @type tag :: String.t()
  @type key_range :: Range.t()
  @type replication_factor :: non_neg_integer()
  @type log_storage_pair :: {log_worker_ref(), StorageWorker.name()}
  @type log_worker_ref :: TransactionLog.worker()

  @spec new(tag(), key_range()) :: t()
  @spec new(tag(), key_range(), replication_factor()) :: t()
  def new(tag, key_range, replication_factor \\ 1) do
    %__MODULE__{
      tag: tag,
      key_range: key_range,
      replication_factor: replication_factor
    }
  end

  @spec storage_workers(t()) :: [StorageWorker.name()]
  def storage_workers(team) do
    team.log_storage_pairs
    |> Enum.map(&elem(&1, 1))
  end
end
