defmodule Bedrock.ControlPlane.Config.LogDescriptor do
  @type t :: %__MODULE__{}
  defstruct [
    # The set of tags that the log services.
    tags: [],

    # The id of the log worker that is responsible for this set of tags.
    log_worker_id: nil
  ]
end
