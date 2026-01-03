defmodule Bedrock.DataPlane.Sequencer.Telemetry do
  @moduledoc """
  Telemetry utilities for sequencer operations.
  """

  alias Bedrock.DataPlane.Version

  @spec emit_next_read_version(Version.t()) :: :ok
  def emit_next_read_version(version) do
    :telemetry.execute(
      [:bedrock, :sequencer, :next_read_version],
      %{},
      %{version: version}
    )
  end

  @spec emit_next_commit_version(Version.t(), Version.t(), non_neg_integer()) :: :ok
  def emit_next_commit_version(last_commit_version, commit_version, elapsed_us) do
    :telemetry.execute(
      [:bedrock, :sequencer, :next_commit_version],
      %{elapsed_us: elapsed_us},
      %{last_commit_version: last_commit_version, commit_version: commit_version}
    )
  end

  @spec emit_successful_commit(Version.t(), Version.t()) :: :ok
  def emit_successful_commit(commit_version, known_committed_version) do
    :telemetry.execute(
      [:bedrock, :sequencer, :successful_commit],
      %{},
      %{commit_version: commit_version, known_committed_version: known_committed_version}
    )
  end
end
