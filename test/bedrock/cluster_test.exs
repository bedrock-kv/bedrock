defmodule Bedrock.ClusterTest do
  use ExUnit.Case
  alias Faker

  alias Bedrock.Cluster

  describe "default_descriptor_file_name/0" do
    test "returns the default descriptor file name" do
      assert Cluster.default_descriptor_file_name() == "bedrock.cluster"
    end
  end

  describe "otp_name/2" do
    test "returns the OTP name for the given cluster and name" do
      name = Faker.Lorem.word()
      service = ~w[airport bus_station train_station taxi_stand]a |> Enum.random()

      assert Cluster.otp_name(name, service) == :"bedrock_#{name}_#{service}"
    end
  end

  describe "__using__/1 with config option" do
    test "creates cluster module with static config" do
      defmodule TestClusterWithConfig do
        use Bedrock.Cluster,
          name: "test_config",
          config: [
            capabilities: [:coordination, :storage],
            coordinator_ping_timeout_in_ms: 500,
            path_to_descriptor: "/tmp/test.cluster"
          ]
      end

      assert TestClusterWithConfig.name() == "test_config"
      assert TestClusterWithConfig.capabilities() == [:coordination, :storage]
      assert TestClusterWithConfig.coordinator_ping_timeout_in_ms() == 500
      assert TestClusterWithConfig.path_to_descriptor() == "/tmp/test.cluster"
    end

    test "creates cluster module with otp_app (backward compatibility)" do
      defmodule TestClusterWithOtpApp do
        use Bedrock.Cluster,
          name: "test_otp_app",
          otp_app: :test_app
      end

      Application.put_env(:test_app, TestClusterWithOtpApp,
        capabilities: [:log],
        coordinator_ping_timeout_in_ms: 1000
      )

      assert TestClusterWithOtpApp.name() == "test_otp_app"
      assert TestClusterWithOtpApp.capabilities() == [:log]
      assert TestClusterWithOtpApp.coordinator_ping_timeout_in_ms() == 1000
    end

    test "raises error when neither otp_app nor config is provided" do
      assert_raise RuntimeError, "Must provide either :otp_app or :config option", fn ->
        defmodule TestClusterWithoutConfig do
          use Bedrock.Cluster, name: "test_no_config"
        end
      end
    end

    test "raises error when name is missing" do
      assert_raise RuntimeError, "Missing :name option", fn ->
        defmodule TestClusterWithoutName do
          use Bedrock.Cluster, config: []
        end
      end
    end

    test "config takes precedence when both otp_app and config are provided" do
      defmodule TestClusterBoth do
        use Bedrock.Cluster,
          name: "test_both",
          otp_app: :test_app,
          config: [capabilities: [:storage]]
      end

      Application.put_env(:test_app, TestClusterBoth, capabilities: [:coordination])

      assert TestClusterBoth.capabilities() == [:storage]
    end

    test "defaults to empty config when only name is provided with config: []" do
      defmodule TestClusterEmpty do
        use Bedrock.Cluster,
          name: "test_empty",
          config: []
      end

      assert TestClusterEmpty.capabilities() == []
      assert TestClusterEmpty.coordinator_ping_timeout_in_ms() == 300
    end
  end
end
