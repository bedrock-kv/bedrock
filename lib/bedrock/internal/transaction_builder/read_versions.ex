defmodule Bedrock.Internal.TransactionBuilder.ReadVersions do
  @moduledoc false

  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.Internal.TransactionBuilder.State

  @type sequencer_fn() :: (pid(), Bedrock.epoch(), keyword() -> {:ok, Bedrock.version()} | {:error, atom()})
  @type next_read_version_fn() :: (State.t() ->
                                     {:ok, Bedrock.version()}
                                     | {:error, atom()})

  @spec next_read_version(State.t()) ::
          {:ok, Bedrock.version()}
          | {:error, :unavailable | :timeout | :unknown | :wrong_epoch}
  @spec next_read_version(State.t(), opts) ::
          {:ok, Bedrock.version()}
          | {:error, :unavailable | :timeout | :unknown | :wrong_epoch}
        when opts: [
               sequencer_fn: sequencer_fn()
             ]
  def next_read_version(t, opts \\ []) do
    sequencer_fn = Keyword.get(opts, :sequencer_fn, &Sequencer.next_read_version/3)
    timeout_in_ms = t.fetch_timeout_in_ms
    epoch = t.transaction_system_layout.epoch

    sequencer_fn.(t.transaction_system_layout.sequencer, epoch, timeout_in_ms: timeout_in_ms)
  end

  @doc """
  Ensures a read version is available on the transaction state.

  If no read version exists, acquires one.
  This is the shared logic used across all fetch operations.

  Returns `{:ok, state}` on success or `{:failure, failures_by_reason}` if
  the read version cannot be acquired (e.g., sequencer unavailable during boot).
  """
  @spec ensure_read_version(
          State.t(),
          opts :: [
            next_read_version_fn: next_read_version_fn()
          ]
        ) :: {:ok, State.t()} | {:failure, %{atom() => []}}
  def ensure_read_version(%{read_version: nil} = t, opts) do
    next_read_version_fn = Keyword.get(opts, :next_read_version_fn, &next_read_version/1)

    case next_read_version_fn.(t) do
      {:ok, read_version} ->
        {:ok, %{t | read_version: read_version}}

      {:error, reason} ->
        {:failure, %{reason => []}}
    end
  end

  def ensure_read_version(t, _opts), do: {:ok, t}

  @doc """
  Ensures a read version is available on the transaction state (bang version).

  Raises if the read version cannot be acquired. Use `ensure_read_version/2` in
  the transaction builder to properly handle failures.
  """
  @spec ensure_read_version!(
          State.t(),
          opts :: [
            next_read_version_fn: next_read_version_fn()
          ]
        ) :: State.t()
  def ensure_read_version!(t, opts) do
    case ensure_read_version(t, opts) do
      {:ok, t} ->
        t

      {:failure, failures_by_reason} ->
        reason = failures_by_reason |> Map.keys() |> hd()
        raise "Failed to acquire read version: #{inspect(reason)}"
    end
  end
end
