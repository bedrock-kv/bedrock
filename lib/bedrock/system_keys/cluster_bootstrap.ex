defmodule Bedrock.SystemKeys.ClusterBootstrap do
  @moduledoc """
  FlatBuffer schema for cluster bootstrap info stored in object storage.

  Object storage key: `{cluster_name}/bootstrap`

  This enables coordinator-free recovery and cold start - nodes read bootstrap
  info from object storage to determine their role and cluster topology.

  ## Fields

    * `cluster_id` - Unique cluster identifier (generated on first boot)
    * `epoch` - Current cluster epoch
    * `logs` - List of log IDs and their last known locations
    * `coordinators` - List of nodes that should run as coordinators

  ## Example

      # Encode
      binary = ClusterBootstrap.to_binary(%{
        cluster_id: "k7m2x9ab",
        epoch: 42,
        logs: [
          %{id: "log-001", otp_ref: %{otp_name: "log_001", node: "node1@host"}},
          %{id: "log-002", otp_ref: %{otp_name: "log_002", node: "node2@host"}}
        ],
        coordinators: [
          %{node: "node1@host"},
          %{node: "node2@host"},
          %{node: "node3@host"}
        ]
      })

      # Decode
      {:ok, bootstrap} = ClusterBootstrap.read(binary)

  ## Boot Sequence

  On node startup:

  1. Read ClusterBootstrap from ObjectStorage
  2. Check if this node is in the coordinators list
  3. If yes → start Coordinator process
  4. If no → run as worker node

  On first boot (no ClusterBootstrap exists), nodes race to create an initial
  bootstrap using conditional PUT. The winner becomes the sole coordinator.

  See `Bedrock.ClusterBootstrap.Discovery` for the discovery logic.
  """

  use Flatbuffer, file: "priv/schemas/cluster_bootstrap.fbs"
end
