defmodule Bedrock.ControlPlane.Coordinator.Commands do
  @moduledoc """
  Structured command types for Raft consensus operations.

  All coordinator operations that require consensus should use these
  structured commands instead of raw data.
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type command ::
          start_epoch_command()
          | update_config_command()
          | update_transaction_system_layout_command()
          | register_node_resources_command()
          | register_services_command()
          | deregister_services_command()

  @type start_epoch_command ::
          {:start_epoch,
           %{
             epoch: Bedrock.epoch(),
             director: pid(),
             relieving: {Bedrock.epoch(), pid()} | {Bedrock.epoch(), :unavailable}
           }}

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

  @type register_node_resources_command ::
          {:register_node_resources,
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
  Create a command to start a new epoch with its director via consensus.
  """
  @spec start_epoch(Bedrock.epoch(), pid(), {Bedrock.epoch(), pid() | :unavailable}) ::
          start_epoch_command()
  def start_epoch(epoch, director, relieving),
    do: {
      :start_epoch,
      %{
        epoch: epoch,
        director: director,
        relieving: relieving
      }
    }

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
  Create a command to register node resources (services and capabilities) via consensus.
  """
  @spec register_node_resources(node(), [service_info()], [Bedrock.Cluster.capability()]) ::
          register_node_resources_command()
  def register_node_resources(node, services, capabilities)
      when is_atom(node) and is_list(services) and is_list(capabilities) do
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

    {
      :register_node_resources,
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
end
