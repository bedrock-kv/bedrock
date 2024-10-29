defmodule Bedrock.ControlPlane.Config.StorageTeamDescriptor do
  alias Bedrock.DataPlane.Storage

  @type vacancy :: {:vacancy, tag :: term()}

  @typedoc """
  ## Fields:
  - `tag`: The tag that identifies the team.
  - `key_range`: The range of keys that the team is responsible for.
  - `ids`: The list of storage workers that are responsible for the team.
  """
  @type t :: %__MODULE__{
          tag: Bedrock.range_tag(),
          key_range: Bedrock.key_range(),
          storage_ids: [Storage.id() | vacancy()]
        }
  defstruct tag: nil,
            key_range: nil,
            storage_ids: []

  @doc """
  Create a new storage team descriptor.
  """
  @spec storage_team_descriptor(Bedrock.range_tag(), Bedrock.key_range(), [
          Storage.id() | vacancy()
        ]) :: t()
  def storage_team_descriptor(tag, key_range, storage_ids) do
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
  def upsert([%{tag: tag} | t], %{tag: tag} = n), do: [n | t]
  def upsert([h | t], n), do: [h | upsert(t, n)]

  @spec find_by_tag([t()], Bedrock.range_tag()) :: t() | nil
  def find_by_tag(l, tag), do: l |> Enum.find(&(&1.tag == tag))

  @spec remove_by_tag([t()], Bedrock.range_tag()) :: [t()]
  def remove_by_tag(l, tag), do: l |> Enum.reject(&(&1.tag == tag))

  @spec update_storage_ids(t, ([Storage.id() | vacancy()] -> [Storage.id() | vacancy()])) :: t()
  def update_storage_ids(t, f), do: %{t | storage_ids: f.(t.storage_ids)}
end
