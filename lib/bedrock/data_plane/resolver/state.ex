defmodule Bedrock.DataPlane.Resolver.State do
  @moduledoc """
  State structure for Resolver GenServer processes.

  Maintains the interval tree for conflict detection, version tracking, and
  waiting queue for out-of-order transactions. Includes lock token for
  authentication.

  ## Metadata Distribution

  The resolver also tracks system metadata mutations (keys with \\xFF prefix)
  and distributes differential window updates to commit proxies:

  - `proxy_progress` - Maps each commit proxy server pid (stable per epoch) to
    `{acked, last_seen}`: the highest window version the proxy has confirmed
    applying (nil if none) and the resolver version at its most recent call.
    Entries not seen within the version retention horizon are expired,
    bounding the map to ~live proxies and unblocking window pruning.
  - `metadata_window` - Accumulated metadata mutations in version order,
    pruned through the minimum confirmed ack across known proxies, capped at
    the retention horizon (no entry younger than the horizon is discarded, so
    a proxy calling within retention never observes a coverage gap)
  - `metadata_pruned_through` - The newest entry version ever discarded by
    pruning (nil if no entry has been discarded). A proxy whose ack falls
    below this floor can no longer be served a complete differential; a proxy
    acked at or above it has confirmed every discarded entry.
  """

  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator

  @type mode :: :running

  @type t :: %__MODULE__{
          conflicts: Conflicts.t(),
          last_version: Bedrock.version(),
          waiting: Bedrock.Internal.WaitingList.t(),
          mode: mode(),
          lock_token: Bedrock.lock_token(),
          epoch: Bedrock.epoch(),
          director: pid(),
          sweep_interval_ms: pos_integer(),
          version_retention_ms: pos_integer(),
          last_sweep_time: integer(),
          proxy_progress: %{
            pid() => {acked :: Bedrock.version() | nil, last_seen :: Bedrock.version()}
          },
          metadata_window: MetadataAccumulator.t(),
          metadata_pruned_through: Bedrock.version() | nil
        }
  defstruct conflicts: nil,
            last_version: nil,
            waiting: %{},
            mode: :running,
            lock_token: nil,
            epoch: nil,
            director: nil,
            sweep_interval_ms: nil,
            version_retention_ms: nil,
            last_sweep_time: nil,
            proxy_progress: %{},
            metadata_window: nil,
            metadata_pruned_through: nil
end
