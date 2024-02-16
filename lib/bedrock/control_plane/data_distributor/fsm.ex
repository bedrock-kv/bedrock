defmodule Bedrock.ControlPlane.DataDistributor.FSM do
  use Gearbox,
    states: ~w(ready)a,
    initial: :starting,
    transitions: %{
      starting: :ready
    }
end
