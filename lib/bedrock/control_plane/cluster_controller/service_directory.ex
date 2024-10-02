defmodule Bedrock.ControlPlane.ClusterController.ServiceDirectory do
  @moduledoc """
  The service directory is a table that keeps track of the services that are
  expected to be running in the cluster. This is used to determine if the cluster
  is in a healthy state.
  """

  @type t :: :ets.table()

  alias Bedrock.ControlPlane.ClusterController.ServiceInfo

  @typep id :: ServiceInfo.id()
  @typep kind :: ServiceInfo.kind()
  @typep expected :: boolean()
  @typep status :: ServiceInfo.status()
  @typep table_row :: {id(), kind(), status(), expected()}

  @spec table_row(ServiceInfo.t(), expected()) :: table_row()
  defp table_row(%{id: id, kind: kind, status: status}, expected),
    do: {id, kind, status, expected}

  @spec new() :: t()
  def new, do: :ets.new(:service_directory, [:set])

  @spec update_service_info(t(), ServiceInfo.t()) :: :changed | :unchanged
  def update_service_info(t, service_info) do
    status = service_info.status

    :ets.lookup(t, service_info.id)
    |> case do
      [{_id, _kind, ^status, _expected}] ->
        :unchanged

      _ ->
        :ets.insert(t, table_row(service_info, false))
        :changed
    end
  end

  @spec node_down(t(), node()) :: [ServiceInfo.t()]
  def node_down(t, node) do
    :ets.select(t, match_id_and_kind_for_running_services_on_node(node))
    |> case do
      [] ->
        []

      affected_rows ->
        affected_rows
        |> Enum.map(fn {id, kind} ->
          :ets.update_element(t, id, {3, :down})
          ServiceInfo.new(id, kind, :down)
        end)
    end
  end

  defp match_id_and_kind_for_running_services_on_node(node),
    do: [{{:"$1", :"$2", {:up, :_, :_, node}, :_}, [], [{{:"$1", :"$2"}}]}]
end
