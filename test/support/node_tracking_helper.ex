defmodule NodeTrackingHelper do
  @moduledoc """
  Test helper utilities for creating mock node tracking tables.
  """

  alias Bedrock.ControlPlane.Director.NodeTracking

  @doc """
  Create a mock node tracking table for testing.

  Creates an ETS table with a single entry for the current node
  with log and storage capabilities.
  """
  @spec create_mock_node_tracking() :: NodeTracking.t()
  def create_mock_node_tracking() do
    node_tracking = :ets.new(:mock_node_tracking, [:ordered_set])
    :ets.insert(node_tracking, [{Node.self(), :unknown, [:log, :storage], :up, true, nil}])
    node_tracking
  end

  @doc """
  Create a mock context with node tracking for recovery phases.
  """
  @spec create_mock_context() :: %{node_tracking: NodeTracking.t()}
  def create_mock_context() do
    %{node_tracking: create_mock_node_tracking()}
  end

  @doc """
  Clean up a mock node tracking table.
  """
  @spec cleanup_mock_node_tracking(NodeTracking.t()) :: :ok
  def cleanup_mock_node_tracking(node_tracking) do
    :ets.delete(node_tracking)
    :ok
  end
end
