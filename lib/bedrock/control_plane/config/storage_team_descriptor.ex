defmodule Bedrock.ControlPlane.Config.StorageTeamDescriptor do
  alias Bedrock.DataPlane.Storage

  @type key_range :: Bedrock.key_range()
  @type tag :: Bedrock.tag()
  @type storage_id :: Storage.id()

  @typedoc """
  ## Fields:
  - `tag`: The tag that identifies the team.
  - `key_range`: The range of keys that the team is responsible for.
  - `ids`: The list of storage workers that are responsible for the team.
  """
  @type t :: %__MODULE__{
          tag: tag(),
          key_range: key_range(),
          storage_ids: [storage_id()]
        }
  defstruct tag: nil,
            key_range: nil,
            storage_ids: []

  @doc """
  Create a new storage team descriptor.
  """
  @spec new(tag(), key_range(), [storage_id()]) :: t()
  def new(tag, key_range, storage_ids) do
    %__MODULE__{
      tag: tag,
      key_range: key_range,
      storage_ids: storage_ids
    }
  end

  @doc """
  Inserts a storage team descriptor into a list of storage team descriptors,
  replacing any existing storage team descriptor with the same tag.
  """
  @spec upsert([t()], t()) :: [t()]
  def upsert([], n), do: [n]
  def upsert([%{tag: tag} | t], n = %{tag: tag}), do: [n | t]
  def upsert([h | t], n), do: [h | upsert(t, n)]

  @spec find_by_tag([t()], tag()) :: t() | nil
  def find_by_tag(l, tag), do: l |> Enum.find(&(&1.tag == tag))

  @spec remove_by_tag([t()], tag()) :: [t()]
  def remove_by_tag(l, tag), do: l |> Enum.reject(&(&1.tag == tag))
end
