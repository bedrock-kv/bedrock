defmodule Bedrock.DataPlane.Sequencer.State do
  @moduledoc """
  State structure for Sequencer GenServer processes.

  Maintains Lamport clock state with three version counters:
  - `known_committed_version`: Highest version confirmed as durably committed (for read requests)
  - `last_commit_version`: Last version handed to a commit proxy
  - `next_commit_version`: Next version to assign to a commit proxy

  The Lamport clock chain is formed by the pair (last_commit_version, next_commit_version).
  """

  @type t() :: %__MODULE__{
          director: pid(),
          epoch: Bedrock.epoch(),
          next_commit_version: Bedrock.version(),
          last_commit_version: Bedrock.version(),
          known_committed_version: Bedrock.version()
        }
  defstruct director: nil,
            epoch: 0,
            next_commit_version: nil,
            last_commit_version: nil,
            known_committed_version: nil
end
