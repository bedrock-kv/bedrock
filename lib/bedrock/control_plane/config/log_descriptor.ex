defmodule Bedrock.ControlPlane.Config.LogDescriptor do
  @moduledoc """
  A `LogDescriptor` is a data structure that describes a log service within the
  system.
  """

  alias Bedrock.DataPlane.Log

  @type tag :: integer()
  @type log_id :: Log.id()

  @typedoc """
  Struct representing a log descriptor.

  ## Fields
    - `tags` - The set of tags that the log services.
    - `log_id` - The id of the log worker that is responsible for this set of
      tags.
  """
  @type t :: %__MODULE__{
          log_id: log_id(),
          tags: [tag()]
        }

  defstruct log_id: nil,
            tags: []

  @doc """
  Creates a new `LogDescriptor` struct.
  """
  @spec new(log_id(), tags :: [tag()]) :: t()
  def new(log_id, tags),
    do: %__MODULE__{
      log_id: log_id,
      tags: tags
    }

  @doc """
  Inserts a log descriptor into a list of log descriptors, replacing any
  existing log descriptor with the same id.
  """
  @spec upsert([t()], t()) :: [t()]
  def upsert([], n), do: [n]
  def upsert([%{log_id: id} | t], n = %{log_id: id}), do: [n | t]
  def upsert([h | t], n), do: [h | upsert(t, n)]

  @spec find_by_id([t()], log_id()) :: t() | nil
  def find_by_id(l, log_id), do: l |> Enum.find(&(&1.log_id == log_id))

  @spec remove_by_id([t()], log_id()) :: [t()]
  def remove_by_id(l, log_id), do: l |> Enum.reject(&(&1.log_id == log_id))
end
