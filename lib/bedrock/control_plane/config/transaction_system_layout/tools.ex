defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout.Tools do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type t :: TransactionSystemLayout.t()
  @type log_id :: Bedrock.service_id()
  @type tag :: Bedrock.tag()

  @spec set_controller(t(), pid()) :: t()
  def set_controller(t, controller) do
    t = put_in(t.id, random_id())
    _t = put_in(t.controller, controller)
  end

  @doc """
  Get a log descriptor by its id or nil if not found.
  """
  @spec find_log_by_id(t(), log_id()) :: LogDescriptor.t() | nil
  def find_log_by_id(t, id),
    do: get_in(t.logs) |> LogDescriptor.find_by_id(id)

  @doc """
  Inserts a log descriptor into the transaction system layout, replacing any
  existing log descriptor with the same id.
  """
  @spec insert_log(t(), LogDescriptor.t()) :: t()
  def insert_log(t, %LogDescriptor{} = descriptor) do
    t = put_in(t.id, random_id())
    _t = update_in(t.logs, &LogDescriptor.upsert(&1, descriptor))
  end

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_log_with_id(t(), log_id()) :: t()
  def remove_log_with_id(t, id) do
    t = put_in(t.id, random_id())
    _t = update_in(t.logs, &LogDescriptor.remove_by_id(&1, id))
  end

  @doc """
  Get a storage team descriptor by its tag or nil if not found.
  """
  @spec find_storage_team_by_tag(t(), tag()) :: LogDescriptor.t() | nil
  def find_storage_team_by_tag(t, tag),
    do: get_in(t.storage_teams) |> StorageTeamDescriptor.find_by_tag(tag)

  @doc """
  Inserts a storage team descriptor into the transaction system layout,
  replacing any existing storage team descriptor with the same tag.
  """
  @spec insert_storage_team(t(), StorageTeamDescriptor.t()) :: t()
  def insert_storage_team(t, %StorageTeamDescriptor{} = descriptor) do
    t = put_in(t.id, random_id())
    _t = update_in(t.storage_teams, &StorageTeamDescriptor.upsert(&1, descriptor))
  end

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_storage_team_with_tag(t(), tag()) :: t()
  def remove_storage_team_with_tag(t, tag) do
    t = put_in(t.id, random_id())
    _t = update_in(t.storage_teams, &StorageTeamDescriptor.remove_by_tag(&1, tag))
  end

  defp random_id, do: Enum.random(1..1_000_000)
end
