defmodule Bedrock.DataPlane.Sequencer do
  @moduledoc """
  Global version authority implementing Lamport clock semantics for MVCC transactions.

  The Sequencer assigns monotonically increasing version numbers that establish
  global ordering for transactions. Read versions provide consistent snapshot points
  while commit versions establish transaction ordering for conflict detection.

  Each commit version assignment updates the Lamport clock, ensuring the version
  pair returned maintains proper causality relationships needed for distributed
  MVCC conflict resolution.

  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @spec next_read_version(
          ref(),
          Bedrock.epoch(),
          opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]
        ) :: {:ok, Bedrock.version()} | {:error, :unavailable | :wrong_epoch}
  def next_read_version(t, epoch, opts \\ []) do
    call(t, {:next_read_version, epoch}, opts[:timeout_in_ms] || :infinity)
  end

  @spec next_commit_version(
          ref(),
          Bedrock.epoch(),
          opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]
        ) ::
          {:ok, last_commit_version :: Bedrock.version(), next_commit_version :: Bedrock.version()}
          | {:error, :unavailable | :wrong_epoch}
  def next_commit_version(t, epoch, opts \\ []) do
    call(t, {:next_commit_version, epoch}, opts[:timeout_in_ms] || :infinity)
  end

  @spec report_successful_commit(
          ref(),
          Bedrock.epoch(),
          commit_version :: Bedrock.version(),
          opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]
        ) :: :ok | {:error, :unavailable | :wrong_epoch}
  def report_successful_commit(t, epoch, commit_version, opts \\ []) do
    call(t, {:report_successful_commit, epoch, commit_version}, opts[:timeout_in_ms] || :infinity)
  end
end
