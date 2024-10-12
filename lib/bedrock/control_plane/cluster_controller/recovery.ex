defmodule Bedrock.ControlPlane.ClusterController.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Config.Mutations,
    only: [
      update_controller: 2,
      update_epoch: 2,
      update_started_at: 2
    ]

  @spec begin_recovery(State.t()) :: State.t()
  def begin_recovery(t) do
    started_at = now()

    update_in(
      t.config,
      &(&1
        |> update_started_at(started_at)
        |> update_epoch(t.epoch)
        |> update_controller(self()))
    )
    |> start_new_recovery_attempt()
  end

  @spec start_new_recovery_attempt(State.t()) :: State.t()
  def start_new_recovery_attempt(t) do
    update_in(t.config.recovery_attempt, fn
      nil ->
        RecoveryAttempt.new(t.config.transaction_system_layout)

      previous_recovery_attempt ->
        RecoveryAttempt.new_from_previous(
          previous_recovery_attempt,
          t.config.transaction_system_layout
        )
    end)
  end

  @spec recover(State.t()) :: State.t()
  def recover(t) do
    t
    |> try_to_invite_old_sequencer()
    |> try_to_invite_old_data_distributor()
    |> try_to_lock_old_logs()
  end

  # Sequencer

  @spec try_to_invite_old_sequencer(State.t()) :: State.t()
  def try_to_invite_old_sequencer(t) do
    t.config
    |> Config.sequencer()
    |> send_rejoin_invitation_to_sequencer(t)
  end

  @spec send_rejoin_invitation_to_sequencer(sequencer :: pid() | nil, State.t()) :: State.t()
  def send_rejoin_invitation_to_sequencer(nil, t), do: t

  def send_rejoin_invitation_to_sequencer(_sequencer, t) do
    # Sequencer.invite_to_rejoin(sequencer, self(), t.epoch)
    # t |> add_expected_service(sequencer, :sequencer)
    t
  end

  # State Distributor

  @spec try_to_invite_old_data_distributor(State.t()) :: State.t()
  def try_to_invite_old_data_distributor(t) do
    t.config
    |> Config.data_distributor()
    |> send_rejoin_invitation_to_data_distributor(t)
  end

  @spec send_rejoin_invitation_to_data_distributor(data_distributor :: pid() | nil, State.t()) ::
          State.t()
  def send_rejoin_invitation_to_data_distributor(nil, t), do: t

  def send_rejoin_invitation_to_data_distributor(_data_distributor, t) do
    # StateDistributor.invite_to_rejoin(data_distributor, self(), t.epoch)
    # t |> add_expected_service(data_distributor, :data_distributor)
    t
  end

  # Transaction Logs

  @spec try_to_lock_old_logs(State.t()) :: State.t()
  def try_to_lock_old_logs(t) do
    # t.config
    # |> Config.logs()
    # |> Enum.reduce(t, fn log_worker, t ->
    #   :ok = Log.request_lock(log_worker, self(), t.epoch)
    #   # t |> add_expected_service(log_worker, :log)
    #   t
    # end)
    t
  end
end
