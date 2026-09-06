defmodule Bedrock.ControlPlane.Director.Publication do
  @moduledoc false
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Director.State

  @retry_ms 1_000
  @max_sends 3

  @spec start(State.t(), map()) :: State.t()
  def start(t, core_state) do
    sequence = t.publication_sequence + 1
    id = t.bootstrap_reservation.recovery_id

    pending = %{
      id: id,
      sequence: sequence,
      layout: t.transaction_system_layout,
      core_state: core_state,
      sends: 1,
      timer: nil
    }

    send_publication(%{t | publication_sequence: sequence, pending_publication: pending})
  end

  @spec retry(State.t(), binary(), non_neg_integer()) :: State.t()
  def retry(%{pending_publication: %{id: id, sequence: sequence} = pending} = t, id, sequence) do
    if pending.sends < @max_sends do
      send_publication(%{t | pending_publication: %{pending | sends: pending.sends + 1}})
    else
      exit({:shutdown, {:recovery_publication_failed, :publication_ack_timeout}})
    end
  end

  def retry(t, _id, _sequence), do: t

  @spec acknowledge(State.t(), Coordinator.ref(), binary(), non_neg_integer()) :: State.t()
  def acknowledge(
        %{pending_publication: %{id: id, sequence: sequence} = pending, coordinator: coordinator} = t,
        coordinator,
        id,
        sequence
      ) do
    Process.cancel_timer(pending.timer)
    %{t | pending_publication: nil}
  end

  def acknowledge(t, _coordinator, _id, _sequence), do: t

  defp send_publication(t) do
    p = t.pending_publication
    Coordinator.notify_transaction_system_layout(t.coordinator, t.epoch, p.sequence, p.id, p.layout, p.core_state)
    timer = Process.send_after(self(), {:publication_retry, p.id, p.sequence}, @retry_ms)
    %{t | pending_publication: %{p | timer: timer}}
  end
end
