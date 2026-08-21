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
          poll_interval_ms: pos_integer()
        }
  @enforce_keys [:cluster, :epoch, :director, :director_monitor, :deps]
  defstruct [
    :cluster,
    :epoch,
    :director,
    :director_monitor,
    :deps,
    lock: nil,
    poll_interval_ms: 5_000
  ]
end
