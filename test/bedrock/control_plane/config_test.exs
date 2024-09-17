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
            ServiceDescriptor.new(:sequencer, :sequencer),
            ServiceDescriptor.new(:data_distributor, :data_distributor)
          ]
        }
      }

      assert [
               %{type: :sequencer, otp_name: :sequencer},
               %{type: :data_distributor, otp_name: :data_distributor}
             ] = Config.service_directory(config)
    end
  end

  describe "sequencer/1" do
    test "returns the sequencer pid" do
      sequencer = spawn(fn -> :ok end)

      config = %Config{
        transaction_system_layout: %TransactionSystemLayout{
          service_directory: [
            ServiceDescriptor.new(:sequencer, sequencer)
          ]
        }
      }

      assert Config.sequencer(config) == sequencer
    end
  end

  describe "data_distributor/1" do
    test "returns the data distributor pid" do
      data_distributor = spawn(fn -> :ok end)

      config = %Config{
        transaction_system_layout: %TransactionSystemLayout{
          service_directory: [
            ServiceDescriptor.new(:data_distributor, data_distributor)
          ]
        }
      }

      assert Config.data_distributor(config) == data_distributor
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
