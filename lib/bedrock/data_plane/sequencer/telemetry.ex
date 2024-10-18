defmodule Bedrock.DataPlane.Sequencer.Telemetry do
  def track_versions(epoch, elapsed_ms, sequence, last_commit_version) do
    :telemetry.execute(
      [:bedrock, :data_plane, :sequencer, :versions],
      %{elapsed_ms: elapsed_ms, sequence: sequence},
      %{
        epoch: epoch,
        last_commit_version: last_commit_version
      }
    )
  end
end
