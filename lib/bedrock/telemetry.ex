defmodule Bedrock.Telemetry do
  @moduledoc false
  @spec execute(
          event_name :: [:bedrock | :storage | :log | :coordinator | atom()],
          measurements :: %{atom() => number()},
          metadata :: %{atom() => term()}
        ) :: :ok
  def execute(event_name, measurements, metadata) do
    :telemetry.execute(event_name, measurements, metadata)
  end
end
