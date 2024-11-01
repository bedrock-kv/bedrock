defmodule Bedrock.DataPlane.Log.Shale.Recovery do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.State

  @spec recover_from(State.t(), Log.ref(), version_vector :: Bedrock.version_vector()) ::
          {:ok, State.t()} | {:error, reason :: term()}
  def recover_from(t, _, _) when t.mode != :locked,
    do: {:error, :lock_required}

  def recover_from(t, nil, {:undefined, 0}) do
    :ets.delete_all_objects(t.log)

    {:ok, %{t | oldest_version: 0, last_version: 0}}
  end

  def recover_from(t, source_log, {min_version, last_version}) do
    :ets.delete_all_objects(t.log)

    case pull_transactions(t.log, source_log, min_version - 1, last_version) do
      :ok -> {:ok, %{t | oldest_version: min_version, last_version: last_version}}
      error -> error
    end
  end

  @spec pull_transactions(
          log :: :ets.table(),
          log_to_pull :: Log.ref(),
          min_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          :ok
          | {:error, :not_ready}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
          | {:error, :version_not_found}
          | {:error, :unavailable}
  def pull_transactions(log, log_to_pull, min_version, last_version) do
    case Log.pull(log_to_pull, min_version, recovery: true, last_version: last_version) do
      {:ok, []} ->
        :ok

      {:ok, transactions} ->
        true = :ets.insert_new(log, transactions)
        pull_transactions(log, log_to_pull, transactions |> List.last() |> elem(0), last_version)

      {:error, reason} ->
        {:error, reason}
    end
  end
end
