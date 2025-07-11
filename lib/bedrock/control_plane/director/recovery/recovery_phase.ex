defmodule Bedrock.ControlPlane.Director.Recovery.RecoveryPhase do
  @moduledoc """
  Behavior for recovery phases in the Bedrock recovery process.

  All recovery phases should implement this behavior to ensure consistent
  interfaces and access to necessary context data.
  """

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.NodeTracking

  @type context :: %{
          node_tracking: NodeTracking.t()
          # Future: Add more context fields as needed
          # cluster: module(),
          # config: Config.t(),
          # etc.
        }

  @doc """
  Execute the recovery phase with the given recovery attempt and context.

  The context provides access to Director state that phases need but
  shouldn't be stored in the recovery attempt itself.
  """
  @callback execute(RecoveryAttempt.t(), context()) :: RecoveryAttempt.t()

  defmacro __using__(_) do
    quote do
      alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
      alias Bedrock.ControlPlane.Config.RecoveryAttempt

      @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
    end
  end
end
