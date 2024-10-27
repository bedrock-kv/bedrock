defmodule Bedrock.Telemetry do
  @spec execute(event_name :: [atom()], metadata :: map(), tags :: map()) :: :ok
  def execute(event_name, metadata, tags) do
    :telemetry.execute(event_name, metadata, tags)
  end
end
