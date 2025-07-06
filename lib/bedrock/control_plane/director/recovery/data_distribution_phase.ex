defmodule Bedrock.ControlPlane.Director.Recovery.DataDistributionPhase do
  @moduledoc """
  Handles the :repair_data_distribution phase of recovery.

  This phase is responsible for repairing data distribution across
  the cluster. Currently, this is a simple transition phase.
  """

  @doc """
  Execute the data distribution phase of recovery.

  Currently just transitions to the next phase. In the future,
  this could handle data rebalancing and distribution repair.
  """
  @spec execute(map()) :: map()
  def execute(%{state: :repair_data_distribution} = recovery_attempt) do
    recovery_attempt |> Map.put(:state, :define_sequencer)
  end
end
