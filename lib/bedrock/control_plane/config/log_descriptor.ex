defmodule Bedrock.ControlPlane.Config.LogDescriptor do
  @moduledoc """
  A `LogDescriptor` is a data structure that describes a log service within the
  system.
  """

  alias Bedrock.DataPlane.Log

  @type tag :: integer()
  @type log_worker_id :: Log.id()

  @typedoc """
  Struct representing a log descriptor.

  ## Fields
    - `tags` - The set of tags that the log services.
    - `log_worker_id` - The id of the log worker that is responsible for this
       set of tags.
  """
  @type t :: %__MODULE__{
          tags: [tag()],
          log_worker_id: log_worker_id()
        }

  defstruct tags: [],
            log_worker_id: nil

  @doc """
  Creates a new `LogDescriptor` struct.
  """
  @spec new(tags :: [tag()], log_worker_id()) :: t()
  def new(tags, log_worker_id),
    do: %__MODULE__{
      tags: tags,
      log_worker_id: log_worker_id
    }
end
