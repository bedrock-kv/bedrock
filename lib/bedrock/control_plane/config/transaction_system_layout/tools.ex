defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout.Tools do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log

  @type t :: TransactionSystemLayout.t()

  @spec set_controller(t(), pid()) :: t()
  def set_controller(t, controller), do: put_in(t.controller, controller) |> update_id()

  @spec set_sequencer(t(), pid()) :: t()
  def set_sequencer(t, sequencer), do: put_in(t.sequencer, sequencer) |> update_id()

  @spec set_data_distributor(t(), pid()) :: t()
  def set_data_distributor(t, data_distributor),
    do: put_in(t.data_distributor, data_distributor) |> update_id()

  @spec set_rate_keeper(t(), pid()) :: t()
  def set_rate_keeper(t, rate_keeper), do: put_in(t.rate_keeper, rate_keeper) |> update_id()

  # Logs

  @doc """
  Get a log descriptor by its id or nil if not found.
  """
  @spec find_log_by_id(t(), Log.id()) :: LogDescriptor.t() | nil
  def find_log_by_id(t, id),
    do: get_in(t.logs) |> LogDescriptor.find_by_id(id)

  @doc """
  Inserts a log descriptor into the transaction system layout, replacing any
  existing log descriptor with the same id.
  """
  @spec upsert_log_descriptor(t(), LogDescriptor.t()) :: t()
  def upsert_log_descriptor(t, %LogDescriptor{} = descriptor),
    do: update_in(t.logs, &LogDescriptor.upsert(&1, descriptor)) |> update_id()

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_log_with_id(t(), Log.id()) :: t()
  def remove_log_with_id(t, id),
    do: update_in(t.logs, &LogDescriptor.remove_by_id(&1, id)) |> update_id()

  # Storage

  @doc """
  Get a storage team descriptor by its tag or nil if not found.
  """
  @spec find_storage_team_by_tag(t(), Bedrock.range_tag()) :: StorageTeamDescriptor.t() | nil
  def find_storage_team_by_tag(t, tag),
    do: get_in(t.storage_teams) |> StorageTeamDescriptor.find_by_tag(tag)

  @doc """
  Inserts a storage team descriptor into the transaction system layout,
  replacing any existing storage team descriptor with the same tag.
  """
  @spec upsert_storage_team_descriptor(t(), StorageTeamDescriptor.t()) :: t()
  def upsert_storage_team_descriptor(t, %StorageTeamDescriptor{} = descriptor),
    do: update_in(t.storage_teams, &StorageTeamDescriptor.upsert(&1, descriptor)) |> update_id()

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_storage_team_with_tag(t(), Bedrock.range_tag()) :: t()
  def remove_storage_team_with_tag(t, tag),
    do: update_in(t.storage_teams, &StorageTeamDescriptor.remove_by_tag(&1, tag)) |> update_id()

  # Services

  @spec set_services(t(), [ServiceDescriptor.t()]) :: t()
  def set_services(t, services) do
    put_in(t.services, services)
    |> update_id()
  end

  @doc """
  Inserts a service descriptor into the transaction system layout, replacing any
  existing service descriptor with the same id.
  """
  @spec upsert_service_descriptor(t(), ServiceDescriptor.t()) :: t()
  def upsert_service_descriptor(t, service_descriptor) do
    update_in(t.services, &ServiceDescriptor.upsert(&1, service_descriptor))
    |> update_id()
  end

  @doc """
  Traverses the list of service descriptors and changes the status of any
  service descriptor that is currently `:up` and running on the given node to
  `:down`.
  """
  @spec node_down(t(), node()) :: t()
  def node_down(t, node) do
    updated_services = Enum.map(t.services, &ServiceDescriptor.node_down(&1, node))

    if updated_services == t.services do
      t
    else
      put_in(t.services, updated_services) |> update_id()
    end
  end

  defp update_id(t), do: put_in(t.id, random_id())

  defp random_id, do: Enum.random(1..1_000_000)
end
