defmodule Bedrock.ControlPlane.Distributor.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Distributor.Lock
  alias Bedrock.ControlPlane.Distributor.Transactions

  @type t :: %__MODULE__{
          cluster: module(),
          epoch: Bedrock.epoch(),
          director: pid(),
          director_monitor: reference(),
          deps: Transactions.deps(),
          lock: Lock.t() | nil,
          poll_interval_ms: pos_integer(),
          placeholder: pid() | nil,
          placeholder_start_fn: (keyword() -> {:ok, pid()} | {:error, term()}) | nil,
          snapshot:
            %{
              shard_layout: %{Bedrock.key() => {Bedrock.range_tag(), Bedrock.key()}},
              materializer_refs: %{Bedrock.range_tag() => {String.t(), String.t()}}
            }
            | nil,
          pending_demands: MapSet.t(Bedrock.range_tag()),
          recruitment_ctx: map() | nil,
          recruiting: MapSet.t(Bedrock.range_tag()),
          recruit_task_refs: %{reference() => Bedrock.range_tag()},
          backoff: %{Bedrock.range_tag() => integer()},
          backoff_ms: pos_integer()
        }
  @enforce_keys [:cluster, :epoch, :director, :director_monitor, :deps]
  defstruct [
    :cluster,
    :epoch,
    :director,
    :director_monitor,
    :deps,
    lock: nil,
    poll_interval_ms: 5_000,
    placeholder: nil,
    placeholder_start_fn: nil,
    snapshot: nil,
    pending_demands: MapSet.new(),
    recruitment_ctx: nil,
    recruiting: MapSet.new(),
    recruit_task_refs: %{},
    backoff: %{},
    backoff_ms: 5_000
  ]
end
