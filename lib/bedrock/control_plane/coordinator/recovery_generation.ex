defmodule Bedrock.ControlPlane.Coordinator.RecoveryGeneration do
  @moduledoc false
  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft

  @io_timeout 5_000

  @spec request(State.t()) :: State.t()
  def request(t) do
    if DirectorManagement.authoritative_leader?(t) and t.director == :unavailable and
         not pending?(t.recovery_generation) do
      request = %{
        request_id: Base.encode16(:crypto.strong_rand_bytes(16)),
        owner_term: t.raft_term,
        phase: :barrier,
        log_id: nil,
        io_id: nil,
        worker: nil,
        monitor: nil,
        timer: nil
      }

      t = %{t | recovery_generation: request, bootstrap_reservation: nil}
      append(t, {:recovery_barrier, Map.take(request, [:request_id, :owner_term])}, :barrier)
    else
      t
    end
  end

  defp pending?(%{phase: phase}), do: phase in [:barrier, :reading, :allocation, :reserving]
  defp pending?(_), do: false

  @spec advance(State.t()) :: State.t()
  def advance(%{recovery_generation: %{phase: phase, log_id: id} = request} = t)
      when phase in [:barrier, :allocation] and id != nil do
    cond do
      not authorized?(t, request) ->
        cancel(t)

      t.last_durable_txn_id < id ->
        t

      phase == :barrier ->
        if is_struct(Raft.log(t.raft), DiskRaftLog) do
          start_io(t, :reading, fn -> Publication.load(t.cluster) end)
        else
          fail(t, :durable_raft_path_required)
        end

      t.last_allocation.request_id == request.request_id ->
        generation = t.last_allocation.generation
        loaded = request.loaded

        start_io(t, :reserving, fn ->
          Publication.reserve(loaded.backend, loaded.key, generation, request.request_id, loaded.bootstrap.cluster_id)
        end)

      true ->
        fail(t, :allocation_superseded)
    end
  end

  def advance(t), do: t

  @spec result(State.t(), reference(), term()) :: State.t()
  def result(%{recovery_generation: %{io_id: id} = request} = t, id, result) when id != nil do
    t = clear_io(t)

    if authorized?(t, request) do
      consume(t, request.phase, result)
    else
      cancel(t)
    end
  end

  def result(t, _id, _result), do: t

  @spec timeout(State.t(), reference()) :: State.t()
  def timeout(%{recovery_generation: %{io_id: id}} = t, id) when id != nil, do: fail(t, :recovery_io_timeout)
  def timeout(t, _id), do: t

  @spec down(State.t(), reference(), term()) :: State.t()
  def down(%{recovery_generation: %{monitor: ref}} = t, ref, reason) when ref != nil,
    do: fail(t, {:recovery_io_down, reason})

  def down(t, _ref, _reason), do: t

  @spec cancel(State.t()) :: State.t()
  def cancel(t) do
    cleared = clear_io(t)
    %{cleared | recovery_generation: nil, bootstrap_reservation: nil}
  end

  defp consume(t, :reading, {:ok, loaded}) do
    generation = max(t.generation_floor, Publication.generation_floor(loaded.bootstrap)) + 1

    cond do
      t.cluster_id != nil and loaded.bootstrap.cluster_id != t.cluster_id ->
        fail(t, :cluster_identity_changed)

      generation > 0xFFFFFFFFFFFFFFFF ->
        fail(t, :recovery_generation_exhausted)

      true ->
        request = Map.put(t.recovery_generation, :loaded, loaded)
        allocation = request |> Map.take([:request_id, :owner_term]) |> Map.put(:generation, generation)

        append(
          %{t | recovery_generation: request, cluster_id: loaded.bootstrap.cluster_id},
          {:begin_recovery, allocation},
          :allocation
        )
    end
  end

  defp consume(t, :reserving, {:ok, reservation}) do
    request = %{t.recovery_generation | phase: :running}

    DirectorManagement.launch_reserved(%{
      t
      | recovery_generation: request,
        bootstrap_reservation: reservation,
        epoch: reservation.generation,
        prior_core_state: CoreState.from_bootstrap(reservation.prior_bootstrap),
        config: Publication.config(reservation.prior_bootstrap, t.cluster)
    })
  end

  defp consume(t, _phase, {:error, reason}), do: fail(t, reason)

  defp append(t, command, phase) do
    case Raft.add_transaction(t.raft, command) do
      {:ok, raft, id} -> %{t | raft: raft, recovery_generation: %{t.recovery_generation | phase: phase, log_id: id}}
      {:error, reason} -> fail(t, reason)
    end
  end

  defp start_io(t, phase, fun) do
    owner = self()
    id = make_ref()
    {pid, monitor} = spawn_monitor(fn -> send(owner, {:recovery_io_result, id, fun.()}) end)
    timeout = Keyword.get(t.cluster.node_config(), :recovery_io_timeout_ms, @io_timeout)
    timer = Process.send_after(owner, {:recovery_io_timeout, id}, timeout)

    %{
      t
      | recovery_generation: %{
          t.recovery_generation
          | phase: phase,
            io_id: id,
            worker: pid,
            monitor: monitor,
            timer: timer
        }
    }
  end

  defp clear_io(%{recovery_generation: nil} = t), do: t

  defp clear_io(t) do
    request = t.recovery_generation
    # Revoke the result identity before signalling; never await native-task DOWN.
    cleared = %{t | recovery_generation: %{request | io_id: nil, worker: nil, monitor: nil, timer: nil}}
    if request.timer, do: Process.cancel_timer(request.timer)
    if request.monitor, do: Process.demonitor(request.monitor, [:flush])
    if request.worker, do: Process.exit(request.worker, :kill)
    cleared
  end

  defp fail(t, reason) do
    t = clear_io(t)

    %{
      t
      | recovery_generation: Map.merge(t.recovery_generation || %{}, %{phase: :failed, reason: reason}),
        bootstrap_reservation: nil,
        leader_startup_state: :recovery_failed
    }
  end

  defp authorized?(t, request), do: DirectorManagement.authoritative_leader?(t) and request.owner_term == t.raft_term
end
