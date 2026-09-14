defmodule Bedrock.ControlPlane.Config.CoreState do
  @moduledoc """
  The durable bootstrap record consumed by recovery.

  Log identities name the previous epoch's durable sources; recovery must
  establish their recoverable history before starting a new epoch. Transient
  addresses and process wiring belong in TransactionSystemLayout instead.

  System materializer names are cached locations for reading bootstrap metadata,
  not durable-history ownership. The distributor can change their committed
  membership between recoveries. Recovery verifies a named cache against the
  membership it reads, or reconstructs tag 0 from recovered logs and chunks when
  the cache is missing or displaced. Losing every materializer costs replay
  work; it does not lose the committed shard layout.
  """

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Worker

  @type t :: %{
          required(:logs) => %{Log.id() => LogDescriptor.t()},
          required(:system_materializers) => %{Worker.id() => node_name :: String.t()}
        }

  @doc """
  Projects the durable cluster-bootstrap record into the prior state
  recovery consumes.

  A log with no recorded tags is carried as `[]` rather than `nil`:
  downstream takes `Map.keys/1` and `MapSet.new/1` over these, so a nil
  would crash a recovery instead of describing a log that serves no
  shard.
  """
  @spec from_bootstrap(map()) :: t()
  def from_bootstrap(bootstrap) do
    logs =
      bootstrap
      |> Map.get(:logs)
      |> Kernel.||([])
      |> Map.new(fn log_info -> {log_info[:id], log_info[:shard_tags] || []} end)

    %{logs: logs, system_materializers: members_from(bootstrap)}
  end

  # Legacy records have no cache hints. Their log history still determines
  # whether to seed a fresh cluster or reconstruct an existing one.
  defp members_from(bootstrap) do
    bootstrap
    |> Map.get(:system_materializers)
    |> Kernel.||([])
    |> Map.new(fn member -> {member[:id], member[:node]} end)
  end

  @doc """
  Projects a completed recovery's layout into the record the NEXT
  recovery reads as its prior state — FDB's `logSystem->toCoreState`,
  which likewise distills a live log system down to what the coordinated
  state must hold.

  Only the durable half crosses the epoch boundary. The layout's pids
  (sequencer, proxies, resolvers) die with the epoch that made them, so
  carrying them into a record whose entire purpose is to OUTLIVE the
  epoch would be a category error — and the reason to keep these two
  types apart at all.

  The system shard's members are passed IN rather than read out of the
  layout, because the layout deliberately carries no membership at all
  ("Nothing O(workers) may ever be added to this broadcast"). The
  director knows them — it just persisted them — so it supplies both
  halves at once.
  """
  @spec from_layout(TransactionSystemLayout.t(), %{Worker.id() => String.t()}) :: t()
  def from_layout(layout, system_materializers),
    do: %{logs: Map.get(layout, :logs) || %{}, system_materializers: system_materializers}

  @doc """
  Preferred cached locations for the system shard.

  These may lag committed coverage changes. Recovery can rebuild the metadata
  view from the independently durable log/chunk history when none is usable.
  """
  @spec system_materializers(t() | nil) :: %{Worker.id() => String.t()}
  def system_materializers(nil), do: %{}
  def system_materializers(core_state), do: Map.get(core_state, :system_materializers) || %{}

  @doc """
  Whether this cluster has never completed a recovery.

  FDB makes the same call on the same evidence, in
  `TagPartitionedLogSystem::recoverAndEndEpoch`
  (`TagPartitionedLogSystem.actor.cpp:2416`): `if (!prevState.tLogs.size())
  { // This is a brand new database` — the branch that MANUFACTURES a
  log system rather than recovering one, keyed on the prior core state
  naming no logs.

  A missing record and a record naming no logs mean the same thing:
  there is no prior epoch's data to recover, so recovery seeds rather
  than reads. Absence of the record is not an error — it is the first
  boot.
  """
  @spec fresh?(t() | nil) :: boolean()
  def fresh?(nil), do: true
  def fresh?(%{logs: logs}) when map_size(logs) == 0, do: true
  def fresh?(_core_state), do: false

  @doc """
  The log ids the prior epoch ran — the services recovery must lock and
  copy from. A fresh cluster names none.
  """
  @spec log_ids(t() | nil | map()) :: MapSet.t(Log.id())
  def log_ids(nil), do: MapSet.new()
  def log_ids(%{logs: logs}), do: logs |> Map.keys() |> MapSet.new()
  # A record without the key names no logs, exactly as every open-coded
  # predecessor of this function assumed. Kept symmetric with fresh?/1:
  # a strict clause here would let fresh?(%{}) route an empty record to
  # the existing-cluster path and then crash the director on it.
  def log_ids(_no_logs_key), do: MapSet.new()
end
