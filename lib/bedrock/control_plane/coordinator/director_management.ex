defmodule Bedrock.ControlPlane.Coordinator.DirectorManagement do
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Director

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [put_director: 2, put_config: 2, put_transaction_system_layout: 2]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_director_changed: 1,
      trace_director_failure_detected: 2,
      trace_director_launch: 2
    ]

  require Logger

  @spec try_to_start_director(State.t()) :: State.t()
  def try_to_start_director(t) when t.leader_node == t.my_node do
    t = t |> maybe_put_defaults()

    trace_director_launch(t.epoch, t.transaction_system_layout)

    {:ok, new_director} = start_director_with_monitoring!(t)

    trace_director_changed(new_director)

    t
    |> put_director(new_director)
  end

  def try_to_start_director(t), do: t

  @spec maybe_put_defaults(State.t()) :: State.t()
  defp maybe_put_defaults(t) do
    t
    |> maybe_put_default_config()
    |> maybe_put_default_transaction_system_layout()
  end

  @spec maybe_put_default_config(State.t()) :: State.t()
  defp maybe_put_default_config(%{config: nil} = t) do
    t
    |> put_config(Config.new(Bedrock.Raft.known_peers(t.raft)))
  end

  defp maybe_put_default_config(t), do: t

  @spec maybe_put_default_transaction_system_layout(State.t()) :: State.t()
  defp maybe_put_default_transaction_system_layout(%{transaction_system_layout: nil} = t) do
    t
    |> put_transaction_system_layout(TransactionSystemLayout.default())
  end

  defp maybe_put_default_transaction_system_layout(t), do: t

  @spec start_director_with_monitoring!(State.t()) ::
          {:ok, pid()} | no_return()
  defp start_director_with_monitoring!(t) do
    t.supervisor_otp_name
    |> DynamicSupervisor.start_child(
      {Director,
       [
         cluster: t.cluster,
         config: t.config,
         old_transaction_system_layout: t.transaction_system_layout,
         epoch: t.epoch,
         coordinator: self(),
         relieving: {t.epoch, t.director},
         services: t.service_directory
       ]}
    )
    |> case do
      {:ok, director_pid} ->
        Process.monitor(director_pid)
        {:ok, director_pid}

      {:error, reason} ->
        raise "Failed to start director: #{inspect(reason)}"
    end
  end

  @spec handle_director_failure(State.t(), director_pid :: pid(), reason :: term()) :: State.t()
  def handle_director_failure(t, director_pid, reason) do
    if t.director == director_pid and t.leader_node == t.my_node do
      trace_director_failure_detected(t.director, reason)
      Logger.warning("Director #{inspect(t.director)} failed with reason: #{inspect(reason)}")

      t
      |> put_director(:unavailable)
    else
      t
    end
  end
end
