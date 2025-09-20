defmodule Bedrock.DataPlane.Resolver.State do
  @moduledoc """
  State structure for Resolver GenServer processes.

  Maintains the interval tree for conflict detection, version tracking, and
  waiting queue for out-of-order transactions. Includes lock token for
  authentication.
  """

  alias Bedrock.DataPlane.Resolver.VersionedConflicts

  @type mode :: :running

  @type t :: %__MODULE__{
          conflicts: VersionedConflicts.t(),
          oldest_version: Bedrock.version(),
          last_version: Bedrock.version(),
          waiting: Bedrock.Internal.WaitingList.t(),
          mode: mode(),
          lock_token: Bedrock.lock_token(),
          epoch: Bedrock.epoch(),
          director: pid(),
          sweep_interval_ms: pos_integer(),
          version_retention_ms: pos_integer(),
          last_sweep_time: integer()
        }
  defstruct conflicts: nil,
            oldest_version: nil,
            last_version: nil,
            waiting: %{},
            mode: :running,
            lock_token: nil,
            epoch: nil,
            director: nil,
            sweep_interval_ms: nil,
            version_retention_ms: nil,
            last_sweep_time: nil
end
