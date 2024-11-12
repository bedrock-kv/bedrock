defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker

  @type state ::
          :start
          | :lock_available_services
          | :determine_old_logs_to_copy
          | :determine_durable_version
          | :recruit_logs_to_fill_vacancies
          | :recruit_storage_to_fill_vacancies
          | :first_time_initialization
          | :create_vacancies
          | :define_sequencer
          | :define_commit_proxies
          | :define_resolvers
          #
          | :replay_old_logs
          | :repair_data_distribution
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
          | {:failed_to_start, :resolver | :commit_proxy | :sequencer, node(), reason :: term()}
          | {:failed_to_playback_logs, %{(log_pid :: pid()) => reason :: term()}}
          | {:failed_to_copy_some_logs,
             [{reason :: term(), new_log_id :: Log.id(), old_log_id :: Log.id()}]}
          | {:need_log_workers, pos_integer()}
          | {:need_storage_workers, pos_integer()}
          | {:insufficient_replication, [Bedrock.range_tag()]}

  @type log_replication_factor :: pos_integer()
  @type storage_replication_factor :: pos_integer()

  @type log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()}
  @type storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}

  @type t :: %{
          state: state(),
          attempt: pos_integer(),
          cluster: module(),
          epoch: non_neg_integer(),
          parameters: %{
            desired_logs: log_replication_factor(),
            desired_replication_factor: storage_replication_factor(),
            desired_commit_proxies: pos_integer()
          },
          started_at: DateTime.t(),
          last_transaction_system_layout: TransactionSystemLayout.t(),
          available_services: %{Worker.id() => ServiceDescriptor.t()},
          locked_service_ids: MapSet.t(Worker.id()),
          log_recovery_info_by_id: log_recovery_info_by_id(),
          storage_recovery_info_by_id: storage_recovery_info_by_id(),
          old_log_ids_to_copy: [Log.id()],
          version_vector: Bedrock.version_vector() | {0, 0},
          durable_version: Bedrock.version(),
          degraded_teams: [Bedrock.range_tag()],
          logs: %{Log.id() => LogDescriptor.t()},
          storage_teams: [StorageTeamDescriptor.t()],
          resolvers: [pid()],
          proxies: [pid()],
          sequencer: pid() | nil
        }
end
