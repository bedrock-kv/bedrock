defmodule Bedrock.ControlPlane.Coordinator.Commands do
  @moduledoc """
  Structured command types for Raft consensus operations.

  All coordinator operations that require consensus should use these
  structured commands instead of raw data.
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type command ::
          end_epoch_command()
          | update_config_command()
          | update_transaction_system_layout_command()
          | set_node_resources_command()
          | merge_node_resources_command()
          | register_services_command()
          | deregister_services_command()

  @type end_epoch_command :: {:end_epoch, Bedrock.epoch() | nil}

  @type update_config_command ::
          {:update_config,
           %{
             config: Config.t()
           }}

  @type update_transaction_system_layout_command ::
          {:update_transaction_system_layout,
           %{
             transaction_system_layout: TransactionSystemLayout.t()
           }}

  @type set_node_resources_command ::
          {:set_node_resources,
           %{
             node: node(),
             services: [service_info()],
             capabilities: [Bedrock.Cluster.capability()]
           }}

  @type merge_node_resources_command ::
          {:merge_node_resources,
           %{
             node: node(),
             services: [service_info()],
             capabilities: [Bedrock.Cluster.capability()]
           }}

  @type register_services_command ::
          {:register_services,
           %{
             services: [service_info()]
           }}

  @type deregister_services_command ::
          {:deregister_services,
           %{
             service_ids: [String.t()]
           }}

  @type service_info :: {service_id :: String.t(), kind :: atom(), worker_ref :: {atom(), node()}}

  @doc """
  Create a command to end the previous epoch via consensus.
  This triggers the initial consensus round needed for director startup.
  """
  @spec end_epoch(Bedrock.epoch() | nil) :: end_epoch_command()
  def end_epoch(previous_epoch), do: {:end_epoch, previous_epoch}

  @doc """
  Create a command to update cluster configuration via consensus.
  """
  @spec update_config(Config.t()) :: update_config_command()
  def update_config(config),
    do: {
      :update_config,
      %{config: config}
    }

  @doc """
  Create a command to update transaction system layout via consensus.
  """
  @spec update_transaction_system_layout(TransactionSystemLayout.t()) ::
          update_transaction_system_layout_command()
  def update_transaction_system_layout(transaction_system_layout),
    do: {
      :update_transaction_system_layout,
      %{transaction_system_layout: transaction_system_layout}
    }

  @doc """
  Create a command to SET node resources (services and capabilities) via consensus.
  SET semantics: completely replaces all resources for the specified node.
  Use for initial node registration or complete resource declarations.
  """
  @spec set_node_resources(node(), [service_info()], [Bedrock.Cluster.capability()]) ::
          set_node_resources_command()
  def set_node_resources(node, services, capabilities)
      when is_atom(node) and is_list(services) and is_list(capabilities) do
    validate_node_resources!(services, capabilities)

    {
      :set_node_resources,
      %{node: node, services: services, capabilities: capabilities}
    }
  end

  @doc """
  Create a command to MERGE node resources (services and capabilities) via consensus.
  MERGE semantics: adds/updates specified resources, preserves existing unspecified resources.
  Use for incremental service registration (e.g., individual worker advertisement).
  """
  @spec merge_node_resources(node(), [service_info()], [Bedrock.Cluster.capability()]) ::
          merge_node_resources_command()
  def merge_node_resources(node, services, capabilities)
      when is_atom(node) and is_list(services) and is_list(capabilities) do
    validate_node_resources!(services, capabilities)

    {
      :merge_node_resources,
      %{node: node, services: services, capabilities: capabilities}
    }
  end

  @doc """
  Create a command to register services via consensus.
  """
  @spec register_services([service_info()]) :: register_services_command()
  def register_services(services) when is_list(services) do
    # Validate service info format
    Enum.each(services, fn
      {service_id, kind, {name, service_node}}
      when is_binary(service_id) and is_atom(kind) and is_atom(name) and is_atom(service_node) ->
        :ok

      invalid ->
        raise ArgumentError,
              "Invalid service info: #{inspect(invalid)}. Expected {service_id, kind, {name, node}}"
    end)

    {
      :register_services,
      %{services: services}
    }
  end

  @doc """
  Create a command to deregister services via consensus.
  """
  @spec deregister_services([String.t()]) :: deregister_services_command()
  def deregister_services(service_ids) when is_list(service_ids) do
    # Validate service IDs
    Enum.each(service_ids, fn
      service_id when is_binary(service_id) -> :ok
      invalid -> raise ArgumentError, "Invalid service ID: #{inspect(invalid)}. Expected string"
    end)

    {
      :deregister_services,
      %{service_ids: service_ids}
    }
  end

  # Private validation helper
  @spec validate_node_resources!([service_info()], [Bedrock.Cluster.capability()]) :: :ok
  defp validate_node_resources!(services, capabilities) do
    # Validate service info format
    Enum.each(services, fn
      {service_id, kind, {name, service_node}}
      when is_binary(service_id) and is_atom(kind) and is_atom(name) and is_atom(service_node) ->
        :ok

      invalid ->
        raise ArgumentError,
              "Invalid service info: #{inspect(invalid)}. Expected {service_id, kind, {name, node}}"
    end)

    # Validate capabilities
    Enum.each(capabilities, fn
      capability when is_atom(capability) -> :ok
      invalid -> raise ArgumentError, "Invalid capability: #{inspect(invalid)}. Expected atom"
    end)

    :ok
  end
end
