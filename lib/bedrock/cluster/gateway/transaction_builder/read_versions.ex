defmodule Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.DataPlane.Sequencer

  @type sequencer_fn() :: (pid() -> {:ok, Bedrock.version()} | {:error, atom()})
  @type next_read_version_fn() :: (State.t() ->
                                     {:ok, Bedrock.version()}
                                     | {:error, atom()})

  @spec next_read_version(State.t()) ::
          {:ok, Bedrock.version()}
          | {:error, :unavailable | :timeout | :unknown}
  @spec next_read_version(State.t(), opts) ::
          {:ok, Bedrock.version()}
          | {:error, :unavailable | :timeout | :unknown}
        when opts: [
               sequencer_fn: sequencer_fn()
             ]
  def next_read_version(t, opts \\ []) do
    sequencer_fn = Keyword.get(opts, :sequencer_fn, &Sequencer.next_read_version/1)

    sequencer_fn.(t.transaction_system_layout.sequencer)
  end

  @doc """
  Ensures a read version is available on the transaction state.

  If no read version exists, acquires one.
  This is the shared logic used across all fetch operations.
  """
  @spec ensure_read_version!(
          State.t(),
          opts :: [
            next_read_version_fn: next_read_version_fn()
          ]
        ) :: State.t()
  def ensure_read_version!(%{read_version: nil} = t, opts) do
    next_read_version_fn = Keyword.get(opts, :next_read_version_fn, &next_read_version/1)

    case next_read_version_fn.(t) do
      {:ok, read_version} ->
        %{t | read_version: read_version}

      {:error, :unavailable} ->
        raise "No read version available"

      {:error, reason} ->
        raise "Failed to acquire read version: #{inspect(reason)}"
    end
  end

  def ensure_read_version!(t, _opts), do: t
end
