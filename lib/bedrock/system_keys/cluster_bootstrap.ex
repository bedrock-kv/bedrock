defmodule Bedrock.SystemKeys.ClusterBootstrap do
  @moduledoc """
  FlatBuffer schema for cluster bootstrap info stored in object storage.

  Object storage key: `bedrock://bootstrap/cluster_identity`

  This enables coordinator-free recovery - the director reads bootstrap
  info from object storage rather than requiring coordinator availability.

  ## Fields

    * `cluster_id` - Unique cluster identifier
    * `epoch` - Current cluster epoch
    * `logs` - List of log IDs and their last known locations
    * `metadata_snapshot_path` - Path to metadata snapshot in object storage

  ## Example

      # Encode
      binary = ClusterBootstrap.to_binary(%{
        cluster_id: "my-cluster",
        epoch: 42,
        logs: [
          %{id: "log-001", otp_ref: %{otp_name: "log_001", node: "node1"}},
          %{id: "log-002", otp_ref: %{otp_name: "log_002", node: "node2"}}
        ],
        metadata_snapshot_path: "bedrock://metadata/snapshots/v1000"
      })

      # Decode
      {:ok, bootstrap} = ClusterBootstrap.read(binary)
  """

  use Flatbuffer, file: "priv/schemas/cluster_bootstrap.fbs"
end
