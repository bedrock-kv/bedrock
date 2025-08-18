defmodule Bedrock.DataPlane.Sequencer do
  @moduledoc """
  Global version authority implementing Lamport clock semantics for MVCC transactions.

  The Sequencer assigns monotonically increasing version numbers that establish
  global ordering for transactions. Read versions provide consistent snapshot points
  while commit versions establish transaction ordering for conflict detection.

  Each commit version assignment updates the Lamport clock, ensuring the version
  pair returned maintains proper causality relationships needed for distributed
  MVCC conflict resolution.

  For detailed version assignment concepts and architectural integration, see the
  [Sequencer documentation](../../../../docs/components/sequencer.md).
  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @spec next_read_version(
          ref(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) :: {:ok, Bedrock.version()} | {:error, :unavailable}
  def next_read_version(t, opts \\ []), do: call(t, :next_read_version, opts[:timeout_in_ms] || :infinity)

  @spec next_commit_version(
          ref(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          {:ok, last_commit_version :: Bedrock.version(), next_commit_version :: Bedrock.version()}
          | {:error, :unavailable}
  def next_commit_version(t, opts \\ []), do: call(t, :next_commit_version, opts[:timeout_in_ms] || :infinity)

  @spec report_successful_commit(ref(), commit_version :: Bedrock.version()) :: :ok
  def report_successful_commit(t, commit_version), do: cast(t, {:report_successful_commit, commit_version})
end
