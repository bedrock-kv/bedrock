defmodule Bedrock.DataPlane.Sequencer.State do
  @type t() :: %__MODULE__{
          controller: pid(),
          epoch: Bedrock.epoch(),
          last_committed_version: Bedrock.version()
        }
  defstruct controller: nil,
            epoch: 0,
            last_committed_version: nil
end
