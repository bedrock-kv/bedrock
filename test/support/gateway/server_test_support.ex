defmodule Bedrock.Test.Gateway.ServerTestSupport do
  @moduledoc """
  Test support utilities for Bedrock.Cluster.Gateway.Server tests.
  Provides common state creation and setup functions.
  """

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Gateway.State

  # Test cluster module used across server tests
  defmodule TestCluster do
    @moduledoc false
    def name, do: "test_cluster"
    def otp_name(:coordinator), do: :test_coordinator
    def otp_name(component), do: :"test_#{component}"
    def coordinator_ping_timeout_in_ms, do: 5000
    def coordinator_discovery_timeout_in_ms, do: 5000
    def gateway_ping_timeout_in_ms, do: 5000
    def capabilities, do: [:storage, :log]
  end

  @doc """
  Creates a base state for testing with optional overrides.
  """
  def base_state(overrides \\ []) do
    defaults = %State{
      node: :test_node,
      cluster: TestCluster,
      descriptor: %Descriptor{coordinator_nodes: [:node1, :node2]},
      path_to_descriptor: "/test/path",
      known_coordinator: :test_coordinator,
      transaction_system_layout: nil,
      mode: :active,
      capabilities: [:storage, :log]
    }

    Map.merge(defaults, Map.new(overrides))
  end

  @doc """
  Creates valid child_spec options with optional overrides.
  """
  def valid_child_spec_opts(overrides \\ []) do
    defaults = [
      cluster: TestCluster,
      descriptor: %Descriptor{coordinator_nodes: [:node1, :node2]},
      path_to_descriptor: "/path/to/descriptor",
      otp_name: :test_gateway,
      capabilities: [:storage, :log]
    ]

    Keyword.merge(defaults, overrides)
  end

  @doc """
  Returns list of required child_spec options.
  """
  def required_child_spec_options do
    [:cluster, :descriptor, :path_to_descriptor, :otp_name, :capabilities]
  end
end
