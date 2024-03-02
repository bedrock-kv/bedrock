defmodule Bedrock.ControlPlane.ClusterController.ServiceDirectory do
  @type t :: :ets.table()

  @spec new() :: t()
  def new, do: :ets.new(:service_directory, [:set])

  @spec add_expected_service(t(), GenServer.name(), atom()) :: :ok
  def add_expected_service(t, service_type, otp_name) do
    if :ets.insert_new(t, {otp_name, service_type, :expected}) do
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
