defmodule Bedrock.ClusterTest do
  use ExUnit.Case

  alias Bedrock.Cluster

  describe "default_descriptor_file_name/0" do
    test "returns the default descriptor file name" do
      assert "bedrock.cluster" = Cluster.default_descriptor_file_name()
    end
  end

  describe "otp_name/2" do
    test "generates valid OTP name format" do
      assert :bedrock_test_cluster_airport = Cluster.otp_name("test_cluster", :airport)
    end

    test "handles different service types" do
      services = [:airport, :bus_station, :train_station, :taxi_stand]

      for service <- services do
        expected = String.to_atom("bedrock_cluster_#{service}")
        assert ^expected = Cluster.otp_name("cluster", service)
      end
    end

    test "handles various cluster names" do
      cluster_names = ["simple", "with_underscore", "123numeric"]

      for name <- cluster_names do
        expected = String.to_atom("bedrock_#{name}_airport")
        assert ^expected = Cluster.otp_name(name, :airport)
      end
    end
  end

  describe "__using__/1 configuration scenarios" do
    test "sets cluster name correctly" do
      defmodule TestClusterName do
        @moduledoc false
        use Cluster, name: "test_config", config: []
      end

      assert "test_config" = TestClusterName.name()
    end

    test "sets node capabilities from static config" do
      defmodule TestClusterCapabilities do
        @moduledoc false
        use Cluster,
          name: "test_capabilities",
          config: [capabilities: [:coordination, :storage]]
      end

      assert [:coordination, :storage] = TestClusterCapabilities.node_capabilities()
    end

    test "sets coordinator ping timeout from static config" do
      defmodule TestClusterTimeout do
        @moduledoc false
        use Cluster,
          name: "test_timeout",
          config: [coordinator_ping_timeout_in_ms: 500]
      end

      assert 500 = TestClusterTimeout.coordinator_ping_timeout_in_ms()
    end

    test "sets path to descriptor from static config" do
      defmodule TestClusterPath do
        @moduledoc false
        use Cluster,
          name: "test_path",
          config: [path_to_descriptor: "/tmp/test.cluster"]
      end

      assert "/tmp/test.cluster" = TestClusterPath.path_to_descriptor()
    end

    test "reads configuration from otp_app (backward compatibility)" do
      defmodule TestClusterWithOtpApp do
        @moduledoc false
        use Cluster,
          name: "test_otp_app",
          otp_app: :test_app
      end

      Application.put_env(:test_app, TestClusterWithOtpApp,
        capabilities: [:log],
        coordinator_ping_timeout_in_ms: 1000
      )

      assert "test_otp_app" = TestClusterWithOtpApp.name()
      assert [:log] = TestClusterWithOtpApp.node_capabilities()
      assert 1000 = TestClusterWithOtpApp.coordinator_ping_timeout_in_ms()
    end

    test "defaults to empty config when config is empty list" do
      defmodule TestClusterEmpty do
        @moduledoc false
        use Cluster,
          name: "test_empty",
          config: []
      end

      assert [] = TestClusterEmpty.node_capabilities()
      assert 300 = TestClusterEmpty.coordinator_ping_timeout_in_ms()
    end
  end

  describe "__using__/1 precedence rules" do
    test "config takes precedence over otp_app when both provided" do
      defmodule TestClusterPrecedence do
        @moduledoc false
        use Cluster,
          name: "test_precedence",
          otp_app: :test_app,
          config: [capabilities: [:storage]]
      end

      Application.put_env(:test_app, TestClusterPrecedence, capabilities: [:coordination])

      assert [:storage] = TestClusterPrecedence.node_capabilities()
    end
  end

  describe "__using__/1 error handling" do
    test "raises error when neither otp_app nor config is provided" do
      assert_raise RuntimeError, "Must provide either :otp_app or :config option", fn ->
        defmodule TestClusterWithoutConfig do
          @moduledoc false
          use Cluster, name: "test_no_config"
        end
      end
    end

    test "raises error when name is missing" do
      assert_raise RuntimeError, "Missing :name option", fn ->
        defmodule TestClusterWithoutName do
          @moduledoc false
          use Cluster, config: []
        end
      end
    end
  end
end
