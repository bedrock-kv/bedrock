defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout.Tools do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log

  @type t :: TransactionSystemLayout.t()

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
    do: t |> update_logs(&LogDescriptor.upsert(&1, descriptor)) |> put_random_id()

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_log_with_id(t(), Log.id()) :: t()
  def remove_log_with_id(t, id),
    do: t |> update_logs(&LogDescriptor.remove_by_id(&1, id)) |> put_random_id()

  # Storage

  @doc """
  Get a storage team descriptor by its tag or nil if not found.
  """
  @spec find_storage_team_by_tag(t(), Bedrock.range_tag()) :: StorageTeamDescriptor.t() | nil
  def find_storage_team_by_tag(t, tag),
    do: t.storage_teams |> StorageTeamDescriptor.find_by_tag(tag)

  @doc """
  Inserts a storage team descriptor into the transaction system layout,
  replacing any existing storage team descriptor with the same tag.
  """
  @spec upsert_storage_team_descriptor(t(), StorageTeamDescriptor.t()) :: t()
  def upsert_storage_team_descriptor(t, %StorageTeamDescriptor{} = descriptor),
    do:
      t |> update_storage_teams(&StorageTeamDescriptor.upsert(&1, descriptor)) |> put_random_id()

  @doc """
  Removes a log descriptor by its id.
  """
  @spec remove_storage_team_with_tag(t(), Bedrock.range_tag()) :: t()
  def remove_storage_team_with_tag(t, tag),
    do:
      t |> update_storage_teams(&StorageTeamDescriptor.remove_by_tag(&1, tag)) |> put_random_id()

  # Services

  @spec set_services(t(), [ServiceDescriptor.t()]) :: t()
  def set_services(t, services), do: %{t | services: services} |> put_random_id()

  @doc """
  Inserts a service descriptor into the transaction system layout, replacing any
  existing service descriptor with the same id.
  """
  @spec upsert_service_descriptor(t(), ServiceDescriptor.t()) :: t()
  def upsert_service_descriptor(t, service_descriptor) do
    t
    |> update_services(&ServiceDescriptor.upsert(&1, service_descriptor))
    |> put_random_id()
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
      t |> put_services(updated_services)
    end
  end

  defp put_random_id(t), do: %{t | id: random_id()}

  @spec put_controller(t(), pid()) :: t()
  def put_controller(t, controller), do: %{t | controller: controller} |> put_random_id()

  @spec put_sequencer(t(), pid()) :: t()
  def put_sequencer(t, sequencer), do: %{t | sequencer: sequencer} |> put_random_id()

  @spec put_data_distributor(t(), pid()) :: t()
  def put_data_distributor(t, data_distributor),
    do: %{t | data_distributor: data_distributor} |> put_random_id()

  @spec put_rate_keeper(t(), pid()) :: t()
  def put_rate_keeper(t, rate_keeper), do: %{t | rate_keeper: rate_keeper} |> put_random_id()

  @spec put_services(t(), [ServiceDescriptor.t()]) :: t()
  defp put_services(t, services), do: %{t | services: services} |> put_random_id()

  @spec random_id() :: TransactionSystemLayout.id()
  defp random_id, do: Enum.random(1..1_000_000)

  @spec update_logs(t(), (LogDescriptor.t() -> [LogDescriptor.t()])) :: t()
  def update_logs(t, updater), do: %{t | logs: updater.(t.logs)}

  @spec update_services(t(), ([ServiceDescriptor.t()] -> [ServiceDescriptor.t()])) ::
          t()
  def update_services(t, updater), do: %{t | services: updater.(t.services)}

  @spec update_storage_teams(t(), ([StorageTeamDescriptor.t()] -> [StorageTeamDescriptor.t()])) ::
          t()
  def update_storage_teams(t, updater), do: %{t | storage_teams: updater.(t.storage_teams)}
end
