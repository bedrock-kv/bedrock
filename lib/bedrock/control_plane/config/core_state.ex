defmodule Bedrock.ControlPlane.Config.CoreState do
  @moduledoc """
  The durable record a recovery recovers FROM — FDB's `DBCoreState`,
  held during recovery as `cstate.prevDBState`.

  This is the other half of a divide FDB keeps in two structures and
  Bedrock, until now, kept in one type:

    * `CoreState` — DURABLE. Persisted (for us, in the object-storage
      cluster bootstrap the coordinator loads at cold start), it names
      what the next recovery must find in order to recover at all.
      FDB's `DBCoreState` (`DBCoreState.h:132`): the tLog sets, old
      generations, `recoveryCount`.
    * `TransactionSystemLayout` — TRANSIENT. Rebuilt every recovery and
      broadcast so workers can reach each other; it carries pids, which
      are meaningless the moment the epoch ends. FDB's `ServerDBInfo`,
      whose own comment reads: "This structure contains transient
      information which is broadcast to all workers for a database,
      permitting them to communicate with each other."

  The rule the split makes structural: what must SURVIVE goes here;
  what must be REACHED goes in the layout. That is why the layout may
  never carry anything `O(workers)` (see its moduledoc) while this
  record may — a thing you must recover is worth persisting, a thing
  you must merely contact is not worth broadcasting.

  ## Why only `logs`

  Recovery consumes exactly one fact from its prior state: which logs
  the last epoch ran, so it can lock them and copy from them. The
  durable bootstrap record also carries the epoch, the cluster id and
  the coordinator set — all read by the coordinator directly, none
  consumed as recovery's prior state — so projecting them here would
  add fields with no reader.

  Log LOCATIONS are likewise absent: the bootstrap schema HAS an
  `otp_ref` per log, but the writer always sets it to nil
  (`persistence_phase.ex`), because recovery discovers live services
  through foreman registration rather than trusting a durable address.
  The record says WHICH logs, never where they were last seen.

  Materializer membership joins this record in bedrock-q67.21.12, for
  the one shard recovery cannot do without: the system shard, whose
  keyspace holds the metadata every later phase reads.
  """

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Log

  @type t :: %{required(:logs) => %{Log.id() => LogDescriptor.t()}}

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

    %{logs: logs}
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
  """
  @spec from_layout(TransactionSystemLayout.t()) :: t()
  def from_layout(layout), do: %{logs: Map.get(layout, :logs) || %{}}

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
