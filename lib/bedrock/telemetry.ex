defmodule Bedrock.Telemetry do
  @spec emit(event_name :: [atom()], metadata :: map(), tags :: map()) :: :ok
  def emit(event_name, metadata, tags) do
    :telemetry.execute(event_name, metadata, tags)
  end
end
