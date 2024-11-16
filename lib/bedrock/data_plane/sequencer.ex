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

  @spec next_read_version(
          ref(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) :: {:ok, Bedrock.version()} | {:error, :unavailable}
  def next_read_version(t, opts \\ []),
    do: t |> call(:next_read_version, opts[:timeout_in_ms] || :infinity)

  @spec next_commit_version(
          ref(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          {:ok, last_commit_version :: Bedrock.version(),
           next_commit_version :: Bedrock.version()}
  def next_commit_version(t, opts \\ []),
    do: t |> call(:next_commit_version, opts[:timeout_in_ms] || :infinity)
end
