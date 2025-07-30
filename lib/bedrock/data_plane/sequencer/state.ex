defmodule Bedrock.DataPlane.Sequencer.State do
  @moduledoc false

  @type t() :: %__MODULE__{
          director: pid(),
          epoch: Bedrock.epoch(),
          last_committed_version: Bedrock.version()
        }
  defstruct director: nil,
            epoch: 0,
            last_committed_version: nil
end
