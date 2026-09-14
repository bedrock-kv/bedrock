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

  ## Relationship to `CoreState`

  This is the WIRE format — the bytes on object storage — and it keeps that
  name because it is read by two callers for two purposes. The boot sequence
  above reads it to answer "what is my role in this cluster", which has
  nothing to do with recovery; recovery reads it through
  `Bedrock.ControlPlane.Config.CoreState.from_bootstrap/1`, which projects
  the subset a recovery consumes as its prior state (`logs` and
  `system_materializers`, FDB's `DBCoreState`). The record is strictly
  larger than that projection: `cluster_id`, `coordinators`, `parameters`
  and `policies` are read by the coordinator directly and never reach
  `CoreState`.

  So the two names name two things, and the relationship is one-way:
  `CoreState` is the in-memory projection of this record, and every change
  to the schema below that recovery must see needs a matching change in
  `from_bootstrap/1`.
  """

  # Flatbuffer 0.6 declares the .fbs file as an @external_resource itself,
  # so a schema edit recompiles this module without further help.
  use Flatbuffer, file: "priv/schemas/cluster_bootstrap.fbs"
end
