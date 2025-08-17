defmodule Bedrock.DataPlane.Storage.Basalt.Logic do
  @moduledoc false
  import Bedrock.DataPlane.Storage.Basalt.State,
    only: [update_mode: 2, update_director_and_epoch: 3, reset_puller: 1, put_puller: 2]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Basalt.Database
  alias Bedrock.DataPlane.Storage.Basalt.Pulling
  alias Bedrock.DataPlane.Storage.Basalt.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Worker

  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  @spec startup(atom(), GenServer.server(), term(), String.t()) ::
          {:ok, State.t()} | {:error, term()}
  def startup(otp_name, foreman, id, path) do
    with :ok <- ensure_directory_exists(path),
         {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets")) do
      {:ok,
       %State{
         path: path,
         otp_name: otp_name,
         id: id,
         foreman: foreman,
         database: database
       }}
    end
  end

  @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
  defp ensure_directory_exists(path), do: File.mkdir_p(path)

  @spec shutdown(State.t()) :: :ok
  def shutdown(%State{} = t), do: :ok = Database.close(t.database)

  @spec lock_for_recovery(State.t(), Director.ref(), Bedrock.epoch()) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists | String.t()}
  @spec lock_for_recovery(State.t(), term(), integer()) :: {:error, :epoch_rollback}
  def lock_for_recovery(t, _, epoch) when not is_nil(t.epoch) and epoch < t.epoch, do: {:error, :newer_epoch_exists}

  @spec lock_for_recovery(State.t(), term(), integer()) :: {:ok, State.t()}
  def lock_for_recovery(t, director, epoch) do
    t
    |> update_mode(:locked)
    |> update_director_and_epoch(director, epoch)
    |> stop_pulling()
    |> then(&{:ok, &1})
  end

  @spec stop_pulling(State.t()) :: State.t()
  def stop_pulling(%{pull_task: nil} = t), do: t

  @spec stop_pulling(State.t()) :: State.t()
  def stop_pulling(%{pull_task: puller} = t) do
    Pulling.stop(puller)
    reset_puller(t)
  end

  @spec unlock_after_recovery(State.t(), Bedrock.version(), TransactionSystemLayout.t()) ::
          {:ok, State.t()}
  @spec unlock_after_recovery(State.t(), term(), map()) :: State.t()
  def unlock_after_recovery(t, durable_version, %{logs: logs, services: services}) do
    with :ok <- Database.purge_transactions_newer_than(t.database, durable_version) do
      puller =
        Pulling.start_pulling(
          durable_version,
          logs,
          services,
          &Database.apply_transactions(t.database, &1),
          fn -> Database.last_durable_version(t.database) end,
          fn -> Database.ensure_durability_within_window(t.database) end
        )

      t
      |> update_mode(:running)
      |> put_puller(puller)
      |> then(&{:ok, &1})
    end
  end

  @spec fetch(State.t(), Bedrock.key(), Version.t()) ::
          {:error, :key_out_of_range | :not_found | :version_too_old} | {:ok, binary()}
  @spec fetch(State.t(), Bedrock.key(), Bedrock.version()) :: Bedrock.value() | :not_found
  def fetch(%State{} = t, key, version), do: Database.fetch(t.database, key, version)

  @spec info(State.t(), Storage.fact_name() | [Storage.fact_name()]) ::
          {:ok, term() | %{Storage.fact_name() => term()}} | {:error, :unsupported_info}
  @spec info(State.t(), atom()) :: term()
  def info(%State{} = t, fact_name) when is_atom(fact_name), do: {:ok, gather_info(fact_name, t)}

  @spec info(State.t(), [Storage.fact_name()]) :: {:ok, %{Storage.fact_name() => term()}}
  def info(%State{} = t, fact_names) when is_list(fact_names) do
    {:ok,
     fact_names
     |> Enum.reduce([], fn
       fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
     end)
     |> Map.new()}
  end

  defp supported_info, do: ~w[
      durable_version
      oldest_durable_version
      id
      pid
      path
      key_ranges
      kind
      n_keys
      otp_name
      size_in_bytes
      supported_info
      utilization
    ]a

  defp gather_info(:oldest_durable_version, t), do: Database.oldest_durable_version(t.database)
  defp gather_info(:durable_version, t), do: Database.last_durable_version(t.database)
  defp gather_info(:id, t), do: t.id
  defp gather_info(:key_ranges, t), do: Database.info(t.database, :key_ranges)
  defp gather_info(:kind, _t), do: :storage
  defp gather_info(:n_keys, t), do: Database.info(t.database, :n_keys)
  defp gather_info(:otp_name, t), do: t.otp_name
  defp gather_info(:path, t), do: t.path
  defp gather_info(:pid, _t), do: self()
  defp gather_info(:size_in_bytes, t), do: Database.info(t.database, :size_in_bytes)
  defp gather_info(:supported_info, _t), do: supported_info()
  defp gather_info(:utilization, t), do: Database.info(t.database, :utilization)
  defp gather_info(_unsupported, _t), do: {:error, :unsupported_info}
end
