defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  @moduledoc """
  Represents an ongoing recovery attempt with its current state and progress.
  """

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Worker

  @type reason_for_stall ::
          :newer_epoch_exists
          | :waiting_for_services
          | :unable_to_meet_log_quorum
          | :no_unassigned_logs
          | :no_unassigned_storage
          | {:source_log_unavailable, log_to_pull :: Log.ref()}
          | {:failed_to_start, :resolver | :commit_proxy | :sequencer, node(),
             reason :: :timeout | :already_started | {:error, start_error()}}
          | {:failed_to_playback_logs, %{(log_pid :: pid()) => reason :: playback_error()}}
          | {:failed_to_copy_some_logs, [{reason :: copy_error(), new_log_id :: Log.id(), old_log_id :: Log.id()}]}
          | {:need_log_workers, pos_integer()}
          | {:need_storage_workers, pos_integer()}
          | {:insufficient_replication, [Bedrock.range_tag()]}
          | {:recovery_system_failed, term()}

  @type log_replication_factor :: pos_integer()
  @type storage_replication_factor :: pos_integer()

  @type start_error :: :process_not_found | :network_timeout | {:exit, any()}
  @type playback_error :: :log_corrupted | :version_mismatch | {:read_failed, atom()}
  @type copy_error :: :source_unavailable | :destination_full | {:transfer_failed, atom()}

  @type log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()}
  @type storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}

  defstruct [
    :attempt,
    :cluster,
    :epoch,
    :started_at,
    :required_services,
    :locked_service_ids,
    :old_log_ids_to_copy,
    :version_vector,
    :durable_version,
    :degraded_teams,
    :logs,
    :storage_teams,
    :resolvers,
    :proxies,
    :sequencer,
    :log_recovery_info_by_id,
    :storage_recovery_info_by_id,
    :transaction_services,
    :service_pids,
    :transaction_system_layout
  ]

  @type t :: %__MODULE__{
          attempt: non_neg_integer(),
          cluster: module(),
          epoch: non_neg_integer(),
          started_at: DateTime.t(),
          required_services: %{Worker.id() => ServiceDescriptor.t()},
          locked_service_ids: MapSet.t(Worker.id()),
          log_recovery_info_by_id: log_recovery_info_by_id(),
          storage_recovery_info_by_id: storage_recovery_info_by_id(),
          old_log_ids_to_copy: [Log.id()],
          version_vector: Bedrock.version_vector() | {0, 0},
          durable_version: Bedrock.version(),
          degraded_teams: [Bedrock.range_tag()],
          logs: %{Log.id() => LogDescriptor.t()},
          storage_teams: [StorageTeamDescriptor.t()],
          resolvers: [{Bedrock.key(), pid()}],
          proxies: [pid()],
          sequencer: pid() | nil,
          transaction_services: %{Worker.id() => ServiceDescriptor.t()},
          service_pids: %{Worker.id() => pid()},
          transaction_system_layout: TransactionSystemLayout.t() | nil
        }

  @doc """
  Creates a new recovery attempt with the required parameters.
  """
  @spec new(cluster :: module(), epoch :: non_neg_integer(), started_at :: DateTime.t()) :: t()
  def new(cluster, epoch, started_at) do
    %__MODULE__{
      attempt: 1,
      cluster: cluster,
      epoch: epoch,
      started_at: started_at,
      required_services: %{},
      locked_service_ids: MapSet.new(),
      log_recovery_info_by_id: %{},
      storage_recovery_info_by_id: %{},
      old_log_ids_to_copy: [],
      version_vector: {Version.zero(), Version.zero()},
      durable_version: Version.zero(),
      degraded_teams: [],
      logs: %{},
      storage_teams: [],
      resolvers: [],
      proxies: [],
      sequencer: nil,
      transaction_services: %{},
      service_pids: %{},
      transaction_system_layout: nil
    }
  end
end
