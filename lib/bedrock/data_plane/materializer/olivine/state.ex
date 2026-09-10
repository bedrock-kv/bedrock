defmodule Bedrock.DataPlane.Materializer.Olivine.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Materializer.Olivine.Reading
  alias Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicy
  alias Bedrock.DataPlane.Materializer.Olivine.SnapshotRetention
  alias Bedrock.ObjectStorage.Snapshot
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  @type t :: %__MODULE__{
          otp_name: atom(),
          path: Path.t(),
          foreman: Foreman.ref(),
          id: Worker.id(),
          shard_id: String.t(),
          database: Database.t(),
          index_manager: IndexManager.t(),
          pull_task: Task.t() | nil,
          epoch: Bedrock.epoch() | nil,
          director: Director.ref() | nil,
          mode: :locked | :running,
          read_request_manager: Reading.t(),
          intake_queue: IntakeQueue.t(),
          window_lag_time_μs: non_neg_integer(),
          compaction_task: Task.t() | nil,
          allow_window_advancement: boolean(),
          snapshot: Snapshot.t() | nil,
          snapshot_policy: SnapshotPolicy.t(),
          snapshot_retention: SnapshotRetention.t(),
          pull_sources: [{Log.id(), Log.ref()}] | nil,
          shard_num: non_neg_integer() | nil,
          idle_timeout: pos_integer() | :infinity,
          last_read_at: integer() | nil,
          known_committed_version: Bedrock.version() | nil,
          pending_ingest: GenServer.from() | nil
        }
  defstruct otp_name: nil,
            path: nil,
            foreman: nil,
            id: nil,
            shard_id: nil,
            database: nil,
            index_manager: nil,
            pull_task: nil,
            epoch: nil,
            director: nil,
            mode: :locked,
            read_request_manager: Reading.new(),
            intake_queue: IntakeQueue.new(),
            window_lag_time_μs: 5_000_000,
            compaction_task: nil,
            allow_window_advancement: true,
            snapshot: nil,
            snapshot_policy: %SnapshotPolicy{},
            snapshot_retention: %SnapshotRetention{},
            pull_sources: nil,
            shard_num: nil,
            idle_timeout: :infinity,
            last_read_at: nil,
            known_committed_version: nil,
            pending_ingest: nil

  @spec update_mode(t(), :locked | :running) :: t()
  def update_mode(t, mode), do: %{t | mode: mode}

  @spec update_director_and_epoch(t(), Director.ref() | nil, Bedrock.epoch() | nil) :: t()
  def update_director_and_epoch(t, director, epoch), do: %{t | director: director, epoch: epoch}

  @spec reset_puller(t()) :: t()
  def reset_puller(t), do: %{t | pull_task: nil}

  @spec put_puller(t(), Task.t()) :: t()
  def put_puller(t, pull_task), do: %{t | pull_task: pull_task}
end
