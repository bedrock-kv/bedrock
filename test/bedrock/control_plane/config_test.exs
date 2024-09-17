defmodule Bedrock.ControlPlane.ConfigTest do
  use ExUnit.Case
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.Policies
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor

  describe "allow_volunteer_nodes_to_join?/1" do
    test "returns the correct value" do
      config = %Config{
        policies: %Policies{allow_volunteer_nodes_to_join: true}
      }

      assert Config.allow_volunteer_nodes_to_join?(config) == true
    end
  end

  describe "nodes/1" do
    test "returns the correct nodes" do
      config = %Config{
        parameters: %{nodes: [:node1, :node2]}
      }

      assert Config.nodes(config) == [:node1, :node2]
    end
  end

  describe "ping_rate_in_ms/1" do
    test "returns the correct ping rate in ms" do
      config = %Config{
        parameters: %{ping_rate_in_hz: 2}
      }

      assert Config.ping_rate_in_ms(config) == 500
    end
  end

  describe "service_directory/1" do
    test "returns the correct service directory" do
      config = %Config{
        transaction_system_layout: %TransactionSystemLayout{
          service_directory: [
            ServiceDescriptor.new(:log, :log1),
            ServiceDescriptor.new(:log, :log2),
            ServiceDescriptor.new(:storage, :storage1),
            ServiceDescriptor.new(:storage, :storage2),
            ServiceDescriptor.new(:storage, :storage3)
          ]
        }
      }

      assert [
               %{type: :log, otp_name: :log1},
               %{type: :log, otp_name: :log2},
               %{type: :storage, otp_name: :storage1},
               %{type: :storage, otp_name: :storage2},
               %{type: :storage, otp_name: :storage3}
             ] = Config.service_directory(config)
    end
  end

  describe "log_workers/1" do
    test "returns the log worker pids" do
      config = %Config{
        transaction_system_layout: %TransactionSystemLayout{
          service_directory: [
            ServiceDescriptor.new(:log, :log1),
            ServiceDescriptor.new(:log, :log2)
          ]
        }
      }

      assert Config.log_workers(config) == [:log1, :log2]
    end
  end
end
