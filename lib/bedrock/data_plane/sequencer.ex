defmodule Bedrock.DataPlane.Sequencer do
  use Bedrock.Internal.GenServerApi

  @type ref :: GenServer.name()

  @spec invite_to_rejoin(
          t :: ref(),
          controller :: pid(),
          Bedrock.epoch(),
          started_at :: Bedrock.timestamp_in_ms(),
          last_committed_version :: Bedrock.version()
        ) :: :ok
  def invite_to_rejoin(t, controller, epoch, started_at, last_committed_version),
    do: t |> cast({:invite_to_rejoin, controller, epoch, started_at, last_committed_version})

  @spec next_read_version(ref(), Bedrock.timeout_in_ms()) :: {:ok, Bedrock.version()}
  def next_read_version(t, timeout_in_ms \\ 5_000),
    do: t |> call(:next_read_version, timeout_in_ms)

  @spec next_read_version(ref(), Bedrock.timeout_in_ms()) ::
          {:ok, last_commit_version :: Bedrock.version(),
           next_commit_version :: Bedrock.version()}
  def next_commit_version(t, timeout_in_ms \\ 5_000),
    do: t |> call(:next_commit_version, timeout_in_ms)
end
