defmodule Bedrock.Cluster.Gateway.DirectorRelations do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.Director
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  use Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Gateway.Telemetry,
    only: [trace_advertising_capabilities: 3, trace_missed_pong: 2, trace_lost_director: 1]

  import Bedrock.Cluster.Gateway.MinimumReadVersions,
    only: [
      recalculate_minimum_read_version: 1
    ]

  @spec change_director(State.t(), {pid(), Bedrock.epoch()} | :unavailable) :: State.t()
  def change_director(t, director) when t.director == director, do: t

  def change_director(t, {_pid, _epoch} = director) do
    t = t |> Map.put(:director, director) |> reset_ping_timer() |> ping_director()

    case t |> advertise_capabilities() do
      {:ok, t} ->
        case t |> fetch_transaction_system_layout() do
          {:ok, t} ->
            t |> publish_director_replaced_to_pubsub()

          {:error, {:relieved_by, {new_epoch, new_director_pid}}} ->
            t |> change_director({new_director_pid, new_epoch})

          {:error, _reason} ->
            t |> change_director(:unavailable)
        end

      {:error, {:relieved_by, {new_epoch, new_director_pid}}} ->
        t |> change_director({new_director_pid, new_epoch})

      {:error, _reason} ->
        t |> change_director(:unavailable)
    end
  end

  def change_director(t, :unavailable) do
    t
    |> Map.put(:director, :unavailable)
    |> Map.put(:transaction_system_layout, nil)
    |> publish_director_replaced_to_pubsub()
  end

  @spec fetch_transaction_system_layout(State.t()) ::
          {:ok, State.t()}
          | {:error, {:relieved_by, {Bedrock.epoch(), pid()}}}
          | {:error, :unavailable | :timeout | :unknown}
  def fetch_transaction_system_layout(t) do
    with {director_pid, _epoch} <- t.director,
         {:ok, transaction_system_layout} <-
           Director.fetch_transaction_system_layout(director_pid, 100) do
      :ets.delete_all_objects(t.storage_table)

      pid_for_storage_id = fn storage_id ->
        transaction_system_layout.services
        |> Map.get(storage_id)
        |> case do
          %{kind: :storage, status: {:up, pid}} ->
            pid

          _ ->
            nil
        end
      end

      Enum.map(transaction_system_layout.storage_teams, fn
        %{tag: tag, key_range: key_range, storage_ids: storage_ids} ->
          Enum.each(storage_ids, fn storage_id ->
            :ets.insert(
              t.storage_table,
              {key_range, storage_id, tag, pid_for_storage_id.(storage_id)}
            )
          end)
      end)

      t
      |> Map.put(:transaction_system_layout, transaction_system_layout)
      |> then(&{:ok, &1})
    else
      :unavailable -> {:error, :unavailable}
      error -> error
    end
  end

  @spec advertise_capabilities(State.t()) ::
          {:ok, State.t()}
          | {:error, :unavailable | :timeout | :unknown}
          | {:error, :nodes_must_be_added_by_an_administrator}
          | {:error, {:relieved_by, {Bedrock.epoch(), director :: pid()}}}
  def advertise_capabilities(t) do
    with {:ok, running_services} <- running_services(t),
         :ok <- trace_advertising_capabilities(t.cluster, t.capabilities, running_services),
         {director_pid, _epoch} <- t.director,
         :ok <-
           Director.request_to_rejoin(
             director_pid,
             Node.self(),
             t.capabilities,
             running_services
           ) do
      {:ok, t}
    else
      error -> error
    end
  end

  @spec advertise_worker_to_director(State.t(), Worker.ref()) :: State.t()
  def advertise_worker_to_director(t, worker_pid) do
    with {:ok, info} <- gather_info_from_worker(worker_pid),
         {director_pid, _epoch} <- t.director,
         :ok <- Director.advertise_worker(director_pid, node(), info) do
      t
    else
      _ -> t
    end
  end

  @spec running_services(State.t()) ::
          {:ok, Director.running_service_info_by_id()}
          | {:error, :unavailable | :timeout | :unknown}
  def running_services(t) do
    case Foreman.all(t.cluster.otp_name(:foreman)) do
      {:ok, worker_pids} -> {:ok, worker_pids |> gather_info_from_workers()}
      {:error, _reason} = error -> error
    end
  end

  @spec gather_info_from_workers([pid()]) :: Director.running_service_info_by_id()
  def gather_info_from_workers(worker_pids) do
    worker_pids
    |> Enum.map(&gather_info_from_worker/1)
    |> Enum.filter(fn
      {:ok, _} -> true
      _ -> false
    end)
    |> Map.new(fn {:ok, info} -> {info[:id], info} end)
  end

  @spec gather_info_from_worker(Worker.ref()) ::
          {:ok, Director.running_service_info()}
          | {:error, :unavailable}
  def gather_info_from_worker(worker),
    do: Worker.info(worker, [:id, :otp_name, :kind, :pid])

  @spec publish_director_replaced_to_pubsub(State.t()) :: State.t()
  def publish_director_replaced_to_pubsub(t) do
    PubSub.publish(
      t.cluster,
      :director_replaced,
      {:director_replaced, t.director}
    )

    t
  end

  @spec ping_director(State.t()) :: State.t()
  def ping_director(t) when t.director == :unavailable, do: t

  def ping_director(t) do
    {director_pid, _epoch} = t.director
    Director.send_ping(director_pid, t.minimum_read_version)
    t
  end

  @spec pong_was_not_received(State.t()) :: State.t()
  def pong_was_not_received(t) when t.missed_pongs > 3 do
    trace_lost_director(t.cluster)

    t
    |> cancel_timer(:ping)
    |> change_director(:unavailable)
  end

  def pong_was_not_received(t) do
    trace_missed_pong(t.cluster, t.missed_pongs)

    t
    |> pong_missed()
    |> reset_ping_timer()
    |> recalculate_minimum_read_version()
    |> ping_director()
  end

  @spec pong_received_from_director(State.t(), {pid(), Bedrock.epoch()} | :unavailable) ::
          State.t()
  def pong_received_from_director(t, {_pid, _epoch} = director)
      when director == t.director do
    t
    |> reset_missed_pongs()
  end

  def pong_received_from_director(t, director) do
    t
    |> reset_missed_pongs()
    |> cancel_all_timers()
    |> change_director(director)
  end

  @spec pong_missed(State.t()) :: State.t()
  def pong_missed(t), do: t |> Map.update!(:missed_pongs, &(&1 + 1))

  @spec reset_missed_pongs(State.t()) :: State.t()
  def reset_missed_pongs(t), do: t |> Map.put(:missed_pongs, 0)

  @spec reset_ping_timer(State.t()) :: State.t()
  def reset_ping_timer(t) do
    t
    |> cancel_timer(:ping)
    |> maybe_set_ping_timer()
  end

  @spec maybe_set_ping_timer(State.t()) :: State.t()
  def maybe_set_ping_timer(%{director: :unavailable} = t), do: t

  def maybe_set_ping_timer(t),
    do: t |> set_timer(:ping, t.cluster.gateway_ping_timeout_in_ms())
end
