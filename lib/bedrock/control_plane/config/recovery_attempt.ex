defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage

  @type state ::
          :start
          | :lock_available_services
          | :determine_suitable_logs
          | :determine_durable_version
          #
          | :recruiting
          | :replaying_logs
          | :repairing_data_distribution
          | :defining_proxies_and_resolvers
          | :final_checks
          | :completed
          | {:stalled, reason_for_stall()}

  @type reason_for_stall ::
          :newer_epoch_exists
          | :waiting_for_services
          | :unable_to_meet_log_quorum
          | {:insufficient_storage, failed_tags :: [Bedrock.range_tag()]}

  @type log_replication_factor :: pos_integer()
  @type storage_replication_factor :: pos_integer()

  @type log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()}
  @type storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}

  @type t :: %__MODULE__{
          state: state(),
          attempt: pos_integer(),
          epoch: non_neg_integer(),
          desired_logs: log_replication_factor(),
          desired_replication_factor: storage_replication_factor(),
          started_at: DateTime.t(),
          last_transaction_system_layout: TransactionSystemLayout.t(),
          available_services: [ServiceDescriptor.t()],
          locked_service_ids: [ServiceDescriptor.id()],
          log_recovery_info_by_id: log_recovery_info_by_id(),
          storage_recovery_info_by_id: storage_recovery_info_by_id(),
          suitable_log_ids: [Log.id()],
          version_vector: Bedrock.version_vector() | :undefined,
          durable_version: Bedrock.version() | :undefined,
          degraded_teams: [Bedrock.range_tag()]
        }
  defstruct state: nil,
            attempt: nil,
            epoch: nil,
            desired_logs: nil,
            desired_replication_factor: nil,
            started_at: nil,
            last_transaction_system_layout: nil,
            available_services: [],
            locked_service_ids: [],
            log_recovery_info_by_id: %{},
            storage_recovery_info_by_id: %{},
            suitable_log_ids: [],
            version_vector: :undefined,
            durable_version: :undefined,
            degraded_teams: []

  @spec new(
          Bedrock.epoch(),
          started_at :: DateTime.t(),
          log_replication_factor(),
          storage_replication_factor(),
          TransactionSystemLayout.t()
        ) :: t()
  def new(epoch, started_at, desired_logs, desired_replication_factor, transaction_system_layout) do
    %__MODULE__{
      attempt: 1,
      epoch: epoch,
      started_at: started_at,
      state: :start,
      desired_logs: desired_logs,
      desired_replication_factor: desired_replication_factor,
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

  @spec put_suitable_log_ids(t(), [Log.id()]) :: t()
  def put_suitable_log_ids(t, suitable_log_ids), do: %{t | suitable_log_ids: suitable_log_ids}

  @spec put_version_vector(t(), Bedrock.version_vector()) :: t()
  def put_version_vector(t, version_vector), do: %{t | version_vector: version_vector}

  @spec put_durable_version(t(), Bedrock.version()) :: t()
  def put_durable_version(t, durable_version), do: %{t | durable_version: durable_version}

  @spec put_degraded_teams(t(), [Bedrock.range_tag()]) :: t()
  def put_degraded_teams(t, degraded_teams), do: %{t | degraded_teams: degraded_teams}

  @spec update_attempt(t(), (pos_integer() -> pos_integer())) :: t()
  def update_attempt(t, f), do: %{t | attempt: f.(t.attempt)}

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
