defmodule Bedrock.ControlPlane.Coordinator.RecoveryCapabilityTracker do
  @moduledoc """
  Tracks recovery capabilities across the cluster.
  """

  alias Bedrock.Cluster

  @type t :: %__MODULE__{
          hash: String.t() | nil
        }

  defstruct hash: nil

  @spec new() :: t()
  def new do
    %__MODULE__{}
  end

  @spec update_hash(t(), String.t()) :: t()
  def update_hash(tracker, hash) do
    %{tracker | hash: hash}
  end

  @spec get_hash(t()) :: String.t() | nil
  def get_hash(tracker) do
    tracker.hash
  end

  @spec check_for_recovery_state_changes(
          t(),
          %{node() => [Cluster.capability()]},
          %{String.t() => {atom(), {atom(), node()}}}
        ) ::
          {:changed | :unchanged, t()}
  def check_for_recovery_state_changes(tracker, node_capabilities, service_directory) do
    current_hash = compute_recovery_state_hash(node_capabilities, service_directory)

    case tracker.hash do
      ^current_hash -> {:unchanged, tracker}
      _ -> {:changed, %{tracker | hash: current_hash}}
    end
  end

  @spec update_recovery_state_hash(
          t(),
          %{node() => [Cluster.capability()]},
          %{String.t() => {atom(), {atom(), node()}}}
        ) :: t()
  def update_recovery_state_hash(tracker, node_capabilities, service_directory) do
    current_hash = compute_recovery_state_hash(node_capabilities, service_directory)
    %{tracker | hash: current_hash}
  end

  # Backward compatibility functions - deprecated, use recovery_state_* versions
  @spec check_for_capability_changes(t(), %{node() => [Cluster.capability()]}) ::
          {:changed | :unchanged, t()}
  def check_for_capability_changes(tracker, node_capabilities) do
    check_for_recovery_state_changes(tracker, node_capabilities, %{})
  end

  @spec update_capability_hash(t(), %{node() => [Cluster.capability()]}) :: t()
  def update_capability_hash(tracker, node_capabilities) do
    update_recovery_state_hash(tracker, node_capabilities, %{})
  end

  defp compute_recovery_state_hash(node_capabilities, service_directory) do
    # Convert maps to sorted lists to ensure deterministic BERT encoding
    capabilities_list =
      node_capabilities
      |> Enum.map(fn {node, caps} -> {node, Enum.sort(caps)} end)
      |> Enum.sort()

    services_list =
      service_directory
      |> Enum.map(fn {service_id, {service_type, {otp_name, node}}} ->
        {service_id, service_type, otp_name, node}
      end)
      |> Enum.sort()

    # Use tuple structure for BERT compatibility
    recovery_state = {capabilities_list, services_list}

    recovery_state
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode32()
  end
end
