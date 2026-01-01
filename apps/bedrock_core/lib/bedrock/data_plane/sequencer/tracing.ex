defmodule Bedrock.DataPlane.Sequencer.Tracing do
  @moduledoc false
  alias Bedrock.DataPlane.Version

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_data_plane_sequencer"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :sequencer, :next_read_version],
        [:bedrock, :sequencer, :next_commit_version],
        [:bedrock, :sequencer, :successful_commit]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  @spec handler(list(atom()), map(), map(), term()) :: :ok
  def handler([:bedrock, :sequencer, event], measurements, metadata, _), do: log_event(event, measurements, metadata)

  @spec log_event(atom(), map(), map()) :: :ok
  def log_event(:next_read_version, _measurements, metadata) do
    info("Next read version: #{Version.to_string(metadata.version)}")
  end

  def log_event(:next_commit_version, measurements, metadata) do
    info(
      "Next commit version: #{Version.to_string(metadata.last_commit_version)} -> #{Version.to_string(metadata.commit_version)} (elapsed: #{measurements.elapsed_us}μs)"
    )
  end

  def log_event(:successful_commit, _measurements, metadata) do
    info(
      "Successful commit reported: #{Version.to_string(metadata.commit_version)}, known committed: #{Version.to_string(metadata.known_committed_version)}"
    )
  end

  defp info(message) do
    Logger.info("Bedrock Sequencer: #{message}", ansi_color: :green)
  end
end
