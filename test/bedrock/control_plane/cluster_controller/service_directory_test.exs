defmodule Bedrock.ControlPlane.ClusterController.ServiceDirectoryTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.ClusterController.ServiceDirectory

  defp with_new(context), do: {:ok, context |> Map.put(:table, ServiceDirectory.new())}

  describe "new/0" do
    test "creates a new service directory" do
      assert is_reference(ServiceDirectory.new())
    end
  end

  describe "add_expected_service!/3" do
    setup :with_new

    test "adds a new expected service", %{table: table} do
      assert :ok == ServiceDirectory.add_expected_service!(table, :my_service, :my_otp_name)
    end

    test "raises an error if the service already exists", %{table: table} do
      ServiceDirectory.add_expected_service!(table, :my_service, :my_otp_name)

      assert_raise RuntimeError, "Service already exists", fn ->
        ServiceDirectory.add_expected_service!(table, :my_service, :my_otp_name)
      end
    end
  end

  describe "any_expected_services?/1" do
    setup :with_new

    test "returns true if there are expected services", %{table: table} do
      ServiceDirectory.add_expected_service!(table, :my_service, :my_otp_name)
      assert ServiceDirectory.any_expected_services?(table)
    end

    test "returns false if there are no expected services", %{table: table} do
      refute ServiceDirectory.any_expected_services?(table)
    end
  end
end
