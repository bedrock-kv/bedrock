defmodule Bedrock.DataPlane.Sequencer.State do
  @moduledoc """
  State structure for Sequencer GenServer processes.

  Maintains Lamport clock state with microsecond-based version counters:
  - `known_committed_version_int`: Highest version confirmed as durably committed (for read requests)
  - `last_commit_version_int`: Last version handed to a commit proxy
  - `next_commit_version_int`: Next version to assign to a commit proxy
  - `epoch_baseline_version_int`: Starting point from previous epoch's last committed version
  - `epoch_start_monotonic_us`: Monotonic time when this epoch started

  Versions are generated using microsecond offsets from the epoch baseline, ensuring
  time-correlated version numbers while maintaining strict monotonic progression.
  The Lamport clock chain is formed by the pair (last_commit_version, next_commit_version).
  """

  @type t() :: %__MODULE__{
          director: pid(),
          epoch: Bedrock.epoch(),
          next_commit_version_int: integer(),
          last_commit_version_int: integer(),
          known_committed_version_int: integer(),
          epoch_baseline_version_int: integer(),
          epoch_start_monotonic_us: integer()
        }
  defstruct director: nil,
            epoch: 0,
            next_commit_version_int: nil,
            last_commit_version_int: nil,
            known_committed_version_int: nil,
            epoch_baseline_version_int: nil,
            epoch_start_monotonic_us: nil
end
