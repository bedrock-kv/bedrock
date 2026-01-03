defmodule Bedrock.ControlPlane.Config.StorageTeamDescriptor do
  @moduledoc """
  Describes a storage team's configuration, including the key range and replicas.
  """

  alias Bedrock.DataPlane.Storage

  @type vacancy :: {:vacancy, tag :: Bedrock.range_tag()}

  @typedoc """
  ## Fields:
  - `tag`: The tag that identifies the team.
  - `key_range`: The range of keys that the team is responsible for.
  - `ids`: The list of storage workers that are responsible for the team.
  """
  @type t :: %{
          tag: Bedrock.range_tag(),
          key_range: Bedrock.key_range(),
          storage_ids: [Storage.id() | vacancy()]
        }

  @doc """
  Create a new storage team descriptor.
  """
  @spec storage_team_descriptor(
          Bedrock.range_tag(),
          Bedrock.key_range(),
          [Storage.id() | vacancy()]
        ) :: t()
  def storage_team_descriptor(tag, key_range, storage_ids),
    do: %{tag: tag, key_range: key_range, storage_ids: storage_ids}

  @doc """
  Inserts a storage team descriptor into a list of storage team descriptors,
  replacing any existing storage team descriptor with the same tag.
  """
  @spec upsert([t()], t()) :: [t()]
  def upsert([], n), do: [n]
  def upsert([%{tag: tag} | t], %{tag: tag} = n), do: [n | t]
  def upsert([h | t], n), do: [h | upsert(t, n)]

  @spec find_by_tag([t()], Bedrock.range_tag()) :: t() | nil
  def find_by_tag(l, tag), do: Enum.find(l, &(&1.tag == tag))

  @spec remove_by_tag([t()], Bedrock.range_tag()) :: [t()]
  def remove_by_tag(l, tag), do: Enum.reject(l, &(&1.tag == tag))
end
