defmodule Bedrock.DataPlane.Resolver.State do
  @moduledoc """
  State structure for Resolver GenServer processes.

  Maintains the interval tree for conflict detection, version tracking, and
  the waiting queue for out-of-order transactions.

  ## Metadata Distribution

  The resolver also records system metadata mutations (keys with \\xFF
  prefix, with this resolver's local verdicts) and relays them to commit
  proxies as exact windows - FDB's stateMutations relay:

  - `last_served` - per-proxy exclusive lower bound of the next window;
    advanced when a window is served. Windows are `(last_served,
    last_version]`, so consecutive windows to one proxy tile exactly.
  - `metadata_window` - accumulated verdict-carrying entries in version
    order, pruned through the minimum served version once every one of the
    epoch's `commit_proxy_count` proxies has been served at least once.
  """

  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator

  @type t :: %__MODULE__{
          conflicts: Conflicts.t(),
          last_version: Bedrock.version(),
          waiting: Bedrock.Internal.WaitingList.t(),
          epoch: Bedrock.epoch(),
          director: pid(),
          sweep_interval_ms: pos_integer(),
          version_retention_ms: pos_integer(),
          last_sweep_time: integer(),
          commit_proxy_count: pos_integer(),
          last_served: %{pid() => Bedrock.version()},
          metadata_window: MetadataAccumulator.t()
        }
  defstruct conflicts: nil,
            last_version: nil,
            waiting: %{},
            epoch: nil,
            director: nil,
            sweep_interval_ms: nil,
            version_retention_ms: nil,
            last_sweep_time: nil,
            # How many commit proxies this epoch runs: pruning waits until
            # every one has been served at least once (FDB's proxy-count gate
            # on oldestProxyVersion pruning), which structurally precludes
            # discarding an entry a not-yet-heard-from proxy still needs.
            commit_proxy_count: nil,
            # The exclusive lower bound of the next window each proxy will be
            # served - advanced when a window is SERVED, not acknowledged
            # (FDB's per-proxy lastVersion). Windows are exact, so a proxy's
            # applied version always equals its last_served here.
            last_served: %{},
            metadata_window: nil
end
