defmodule Bedrock.ControlPlane.Config.StorageTeamDescriptor do
  @type t :: %__MODULE__{}
  defstruct [
    # The first key in the range of keys that the team is responsible for.
    start_key: nil,

    # The tag that identifies the team.
    tag: nil,

    # The list of storage workers that are responsible for the team.
    storage_worker_ids: []
  ]
end
