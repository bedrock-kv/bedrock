defmodule Bedrock.DataPlane.Log.Limestone.State do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Limestone.Subscriptions
  alias Bedrock.DataPlane.Log.Limestone.StateMachine
  alias Bedrock.DataPlane.Log.Limestone.Transactions
  alias Bedrock.Service.Foreman

  @type state :: :starting | :locked | :ready
  @type t :: %__MODULE__{
          state: state(),
          subscriber_liveness_timeout_in_s: pos_integer(),
          id: Log.id(),
          otp_name: atom(),
          transactions: Transactions.t(),
          foreman: Foreman.ref() | nil,
          epoch: Bedrock.epoch() | nil,
          subscriptions: Subscriptions.t(),
          oldest_version: Bedrock.version(),
          last_version: Bedrock.version(),
          cluster_controller: ClusterController.ref() | nil
        }
  defstruct state: nil,
            subscriber_liveness_timeout_in_s: 60,
            id: nil,
            otp_name: nil,
            transactions: nil,
            foreman: nil,
            epoch: nil,
            subscriptions: nil,
            oldest_version: nil,
            last_version: nil,
            cluster_controller: nil

  @type transition_fn :: (t() -> t())

  @spec transition_to(t(), state(), transition_fn()) :: {:ok, t()} | {:error, any()}
  def transition_to(t, new_state, transition_fn) do
    Gearbox.transition(t, StateMachine, new_state)
    |> case do
      {:ok, new_t} ->
        transition_fn.(new_t)
        |> case do
          {:halt, reason} -> {:error, reason}
          new_t -> {:ok, new_t}
        end

      error ->
        error
    end
  end
end
