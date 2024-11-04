defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage

  @type state ::
          :start
          | :lock_available_services
          | :determine_old_logs_to_copy
          | :determine_durable_version
          | :recruit_logs_to_fill_vacancies
          | :recruit_storage_to_fill_vacancies
          | :first_time_initialization
          | :create_vacancies
          #
          | :replay_old_logs
          | :repair_data_distribution
          | :defining_proxies_and_resolvers
          | :final_checks
          | :completed
          | {:stalled, reason_for_stall()}

  @type reason_for_stall ::
          :newer_epoch_exists
          | :waiting_for_services
          | :unable_to_meet_log_quorum
          | :no_unassigned_logs
          | :no_unassigned_storage
          | {:source_log_unavailable, log_to_pull :: Log.ref()}
          | {:failed_to_start_sequencer, reason :: term()}
          | {:failed_to_playback_logs, %{(log_pid :: pid()) => reason :: term()}}
          | {:failed_to_copy_some_logs,
             [{reason :: term(), new_log_id :: Log.id(), old_log_id :: Log.id()}]}
          | {:failed_to_start_proxy, node(), reason :: term()}
          | {:need_log_workers, pos_integer()}
          | {:need_storage_workers, pos_integer()}
          | {:insufficient_replication, [Bedrock.range_tag()]}

  @type log_replication_factor :: pos_integer()
  @type storage_replication_factor :: pos_integer()

  @type log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()}
  @type storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}

  @type t :: %__MODULE__{
          state: state(),
          attempt: pos_integer(),
          cluster: module(),
          epoch: non_neg_integer(),
          parameters: %{
            desired_logs: log_replication_factor(),
            desired_replication_factor: storage_replication_factor(),
            desired_commit_proxies: pos_integer(),
            desired_resolvers: pos_integer()
          },
          started_at: DateTime.t(),
          last_transaction_system_layout: TransactionSystemLayout.t(),
          available_services: [ServiceDescriptor.t()],
          locked_service_ids: MapSet.t(ServiceDescriptor.id()),
          log_recovery_info_by_id: log_recovery_info_by_id(),
          storage_recovery_info_by_id: storage_recovery_info_by_id(),
          old_log_ids_to_copy: [Log.id()],
          version_vector: Bedrock.version_vector() | {:start, 0},
          durable_version: Bedrock.version() | :start,
          degraded_teams: [Bedrock.range_tag()],
          logs: [LogDescriptor.t()],
          storage_teams: [StorageTeamDescriptor.t()],
          resolvers: [pid()],
          proxies: [pid()],
          sequencer: pid() | nil
        }
  defstruct state: nil,
            attempt: nil,
            epoch: nil,
            cluster: nil,
            parameters: nil,
            started_at: nil,
            last_transaction_system_layout: nil,
            available_services: [],
            locked_service_ids: MapSet.new(),
            log_recovery_info_by_id: %{},
            storage_recovery_info_by_id: %{},
            old_log_ids_to_copy: [],
            version_vector: {:start, 0},
            durable_version: :start,
            degraded_teams: [],
            logs: [],
            storage_teams: [],
            resolvers: [],
            proxies: [],
            sequencer: nil

  @spec new(
          cluster :: module(),
          Bedrock.epoch(),
          started_at :: DateTime.t(),
          TransactionSystemLayout.t(),
          params :: %{
            desired_logs: log_replication_factor(),
            desired_replication_factor: storage_replication_factor(),
            desired_commit_proxies: pos_integer(),
            desired_resolvers: pos_integer()
          }
        ) :: t()
  def new(cluster, epoch, started_at, transaction_system_layout, params) do
    %__MODULE__{
      cluster: cluster,
      attempt: 1,
      epoch: epoch,
      started_at: started_at,
      state: :start,
      parameters: params,
      last_transaction_system_layout: transaction_system_layout
    }
  end

  def reset(t, reset_at) do
    t
    |> update_attempt(&(&1 + 1))
    |> put_started_at(reset_at)
    |> put_state(:start)
  end

  @spec stalled?(t()) :: boolean()
  def stalled?(%{state: {:stalled, _}}), do: true
  def stalled?(_), do: false

  @spec put_state(t(), state()) :: t()
  def put_state(t, state), do: %{t | state: state}

  @spec put_started_at(t(), DateTime.t()) :: t()
  def put_started_at(t, started_at), do: %{t | started_at: started_at}

  @spec put_old_log_ids_to_copy(t(), [Log.id()]) :: t()
  def put_old_log_ids_to_copy(t, old_log_ids_to_copy),
    do: %{t | old_log_ids_to_copy: old_log_ids_to_copy}

  @spec put_version_vector(t(), Bedrock.version_vector()) :: t()
  def put_version_vector(t, version_vector), do: %{t | version_vector: version_vector}

  @spec put_durable_version(t(), Bedrock.version() | :start) :: t()
  def put_durable_version(t, durable_version), do: %{t | durable_version: durable_version}

  @spec put_degraded_teams(t(), [Bedrock.range_tag()]) :: t()
  def put_degraded_teams(t, degraded_teams), do: %{t | degraded_teams: degraded_teams}

  @spec update_attempt(t(), (pos_integer() -> pos_integer())) :: t()
  def update_attempt(t, f), do: %{t | attempt: f.(t.attempt)}

  @spec put_logs(t(), [LogDescriptor.t()]) :: t()
  def put_logs(t, logs), do: %{t | logs: logs}

  @spec put_storage_teams(t(), [StorageTeamDescriptor.t()]) :: t()
  def put_storage_teams(t, storage_teams), do: %{t | storage_teams: storage_teams}

  @spec put_resolvers(t(), [pid()]) :: t()
  def put_resolvers(t, resolvers), do: %{t | resolvers: resolvers}

  @spec put_proxies(t(), [pid()]) :: t()
  def put_proxies(t, proxies), do: %{t | proxies: proxies}

  @spec put_sequencer(t(), pid()) :: t()
  def put_sequencer(t, sequencer), do: %{t | sequencer: sequencer}

  @spec update_log_recovery_info_by_id(
          t(),
          (log_recovery_info_by_id() -> log_recovery_info_by_id())
        ) :: t()
  def update_log_recovery_info_by_id(t, f),
    do: %{t | log_recovery_info_by_id: f.(t.log_recovery_info_by_id)}

  @spec update_storage_recovery_info_by_id(
          t(),
          (storage_recovery_info_by_id() -> storage_recovery_info_by_id())
        ) :: t()
  def update_storage_recovery_info_by_id(t, f),
    do: %{t | storage_recovery_info_by_id: f.(t.storage_recovery_info_by_id)}

  @spec put_available_services(t(), [ServiceDescriptor.t()]) :: t()
  def put_available_services(t, available_services),
    do: %{t | available_services: available_services}

  @spec update_available_services(t(), ([ServiceDescriptor.t()] -> [ServiceDescriptor.t()])) ::
          t()
  def update_available_services(t, f),
    do: %{t | available_services: f.(t.available_services)}

  @spec update_locked_service_ids(t(), ([ServiceDescriptor.id()] -> [ServiceDescriptor.id()])) ::
          t()
  def update_locked_service_ids(t, f),
    do: %{t | locked_service_ids: f.(t.locked_service_ids)}
end
