defmodule Bedrock.DataPlane.Sequencer.State do
  @moduledoc false

  @type t() :: %__MODULE__{
          director: pid(),
          epoch: Bedrock.epoch(),
          next_commit_version: Bedrock.version(),
          read_version: Bedrock.version()
        }
  defstruct director: nil,
            epoch: 0,
            next_commit_version: nil,
            read_version: nil
end
