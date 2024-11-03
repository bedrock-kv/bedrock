defmodule Bedrock.DataPlane.Log.Shale.Recovery do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.State

  @spec recover_from(
          State.t(),
          Log.ref(),
          first_version :: Bedrock.version() | :undefined,
          last_version :: Bedrock.version_vector()
        ) ::
          {:ok, State.t()} | {:error, reason :: term()}
  def recover_from(t, _, _, _) when t.mode != :locked,
    do: {:error, :lock_required}

  def recover_from(t, _, :undefined, 0) do
    :ets.delete_all_objects(t.log)
    :ets.insert(t.log, Log.initial_transaction())
    {:ok, %{t | mode: :running, oldest_version: :undefined, last_version: 0}}
  end

  def recover_from(t, source_log, first_version, last_version) do
    :ets.delete_all_objects(t.log)

    if first_version == :undefined do
      :ets.insert(t.log, Log.initial_transaction())
    end

    case pull_transactions(t.log, source_log, first_version, last_version) do
      :ok ->
        {:ok, %{t | mode: :running, oldest_version: first_version, last_version: last_version}}

      error ->
        error
    end
  end

  @spec pull_transactions(
          log :: :ets.table(),
          log_to_pull :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          :ok | Log.pull_errors() | {:error, {:source_log_unavailable, log_to_pull :: Log.ref()}}
  def pull_transactions(_, _, first_version, last_version)
      when first_version == last_version,
      do: :ok

  def pull_transactions(log, log_to_pull, first_version, last_version) do
    case Log.pull(log_to_pull, first_version, recovery: true, last_version: last_version) do
      {:ok, []} ->
        :ok

      {:ok, transactions} ->
        true = :ets.insert_new(log, transactions)
        pull_transactions(log, log_to_pull, transactions |> List.last() |> elem(0), last_version)

      {:error, :unavailable} ->
        {:error, {:source_log_unavailable, log_to_pull}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
