defmodule Bedrock.DataPlane.Sequencer do
  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.name()

  @spec invite_to_rejoin(
          t :: ref(),
          director :: pid(),
          Bedrock.epoch(),
          last_committed_version :: Bedrock.version()
        ) :: :ok
  def invite_to_rejoin(t, director, epoch, last_committed_version),
    do: t |> cast({:invite_to_rejoin, director, epoch, last_committed_version})

  @spec next_read_version(ref()) :: {:ok, Bedrock.version()}
  def next_read_version(t),
    do: t |> call(:next_read_version, :infinity)

  @spec next_commit_version(ref()) ::
          {:ok, last_commit_version :: Bedrock.version(),
           next_commit_version :: Bedrock.version()}
  def next_commit_version(t),
    do: t |> call(:next_commit_version, :infinity)
end
