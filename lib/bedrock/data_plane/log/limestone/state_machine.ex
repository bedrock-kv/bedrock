defmodule Bedrock.DataPlane.Log.Limestone.StateMachine do
  use Gearbox,
    states: [:starting, :locked, :ready],
    initial: :starting,
    transitions: %{
      starting: :locked,
      locked: [:locked, :ready],
      ready: :locked
    }
end
