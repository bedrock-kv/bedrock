defmodule Bedrock.ControlPlane.ClusterController.ServiceDirectory do
  @moduledoc """
  The service directory is a table that keeps track of the services that are
  expected to be running in the cluster. This is used to determine if the cluster
  is in a healthy state.
  """

  @type t :: :ets.table()

  @typep service :: GenServer.name()
  @typep service_type :: atom()
  @typep status :: atom()

  @spec row(service(), service_type(), status()) :: {service(), service_type(), status()}
  defp row(otp_name, service_type, status), do: {otp_name, service_type, status}

  @spec new() :: t()
  def new, do: :ets.new(:service_directory, [:set])

  @spec add_expected_service!(t(), GenServer.name(), atom()) :: :ok
  def add_expected_service!(t, service_type, otp_name) do
    if :ets.insert_new(t, row(otp_name, service_type, :expected)) do
      :ok
    else
      raise "Service already exists"
    end
  end

  @spec any_expected_services?(t()) :: boolean()
  def any_expected_services?(t) do
    :ets.select(t, [{{:_, :_, :expected}, [], [true]}]) != []
  end
end
