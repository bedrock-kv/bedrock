defmodule Bedrock.ControlPlane.Config.StorageTeamDescriptor do
  alias Bedrock.DataPlane.Storage

  @typedoc """
  ## Fields:
  - `start_key`: The first key in the range of keys that the team is responsible for.
  - `tag`: The tag that identifies the team.
  - `storage_worker_ids`: The list of storage workers that are responsible for the team.
  """
  @type t :: %__MODULE__{
          key_range: Bedrock.key_range(),
          tag: integer(),
          storage_worker_ids: [Storage.id()]
        }
  defstruct key_range: nil,
            tag: nil,
            storage_worker_ids: []

  def new(key_range, tag, storage_worker_ids) do
    %__MODULE__{
      key_range: key_range,
      tag: tag,
      storage_worker_ids: storage_worker_ids
    }
  end
end
