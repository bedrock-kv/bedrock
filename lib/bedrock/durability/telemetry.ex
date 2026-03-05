defmodule Bedrock.Durability.Telemetry do
  @moduledoc false
  alias Bedrock.Durability.Profile
  alias Bedrock.Telemetry

  @ok_event [:bedrock, :durability, :profile, :ok]
  @failed_event [:bedrock, :durability, :profile, :failed]

  @spec trace_profile(Profile.t(), map()) :: :ok
  def trace_profile(%Profile{} = profile, metadata \\ %{}) do
    event = if profile.status == :ok, do: @ok_event, else: @failed_event

    Telemetry.execute(event, %{failed_checks: length(profile.reasons)}, Map.merge(base_metadata(profile), metadata))
  end

  defp base_metadata(profile) do
    %{
      status: profile.status,
      reasons: profile.reasons
    }
  end
end
