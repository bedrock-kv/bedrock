defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  @moduledoc """
  Represents an ongoing recovery attempt with its current state and progress.
  """

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Worker

  @type reason_for_stall ::
          :newer_epoch_exists
          | :waiting_for_services
          | :unable_to_meet_log_quorum
          | :no_unassigned_logs
          | {:source_log_unavailable, log_to_pull :: Log.ref()}
          | {:failed_to_start, :resolver | :commit_proxy | :sequencer, node(),
             reason :: :timeout | :already_started | {:error, start_error()}}
          | {:failed_to_playback_logs, %{(log_pid :: pid()) => reason :: playback_error()}}
          | {:failed_to_copy_some_logs, [{reason :: copy_error(), new_log_id :: Log.id(), old_log_id :: Log.id()}]}
          | {:need_log_workers, pos_integer()}
          | {:materializer_unavailable, term()}
          | {:recovery_system_failed, term()}

  @type log_replication_factor :: pos_integer()

  @type start_error :: :process_not_found | :network_timeout | {:exit, any()}
  @type playback_error :: :log_corrupted | :version_mismatch | {:read_failed, atom()}
  @type copy_error :: :source_unavailable | :destination_full | {:transfer_failed, atom()}

  @type log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()}
  @type materializer_recovery_info_by_id :: %{Materializer.id() => Materializer.recovery_info()}

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
    :logs,
    :resolvers,
    :proxies,
    :sequencer,
    :log_recovery_info_by_id,
    :materializer_recovery_info_by_id,
    :transaction_services,
    :service_pids,
    :transaction_system_layout,
    :metadata_materializer,
    :shard_layout
  ]

  @type shard_layout :: %{Bedrock.key() => {Bedrock.range_tag(), Bedrock.key()}}

  @type t :: %__MODULE__{
          attempt: non_neg_integer(),
          cluster: module(),
          epoch: non_neg_integer(),
          started_at: DateTime.t(),
          required_services: %{Worker.id() => ServiceDescriptor.t()},
          locked_service_ids: MapSet.t(Worker.id()),
          log_recovery_info_by_id: log_recovery_info_by_id(),
          materializer_recovery_info_by_id: materializer_recovery_info_by_id(),
          old_log_ids_to_copy: [Log.id()],
          version_vector: Bedrock.version_vector() | {0, 0},
          durable_version: Bedrock.version(),
          logs: %{Log.id() => LogDescriptor.t()},
          resolvers: [{Bedrock.key(), pid()}],
          proxies: [pid()],
          sequencer: pid() | nil,
          transaction_services: %{Worker.id() => ServiceDescriptor.t()},
          service_pids: %{Worker.id() => pid()},
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          metadata_materializer: pid() | nil,
          shard_layout: shard_layout() | nil
        }

  @doc """
  Returns the shard ID for system/metadata keys (tag 0).

  The system shard contains all keys starting with 0xFF, including the shard layout
  stored at `\\xff/system/shard_keys/*`.
  """
  @spec system_shard_id() :: non_neg_integer()
  def system_shard_id, do: 0

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
      materializer_recovery_info_by_id: %{},
      old_log_ids_to_copy: [],
      version_vector: {Version.zero(), Version.zero()},
      durable_version: Version.zero(),
      logs: %{},
      resolvers: [],
      proxies: [],
      sequencer: nil,
      transaction_services: %{},
      service_pids: %{},
      transaction_system_layout: nil,
      metadata_materializer: nil,
      shard_layout: nil
    }
  end
end
